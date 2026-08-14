#include "common/validation.h"
#include "schemeshard_impl.h"
#include "schemeshard_info_types.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/tiering/tier/object.h>

namespace NKikimr {
namespace NSchemeShard {

namespace {
static inline bool IsDropped(const TTableInfo::TColumn& col) {
    return col.IsDropped();
}

static inline NScheme::TTypeInfo GetType(const TTableInfo::TColumn& col) {
    return col.PType;
}

}

bool ValidateTtlSettings(const NKikimrSchemeOp::TTTLSettings& ttl,
    const TMap<ui32, TTableInfo::TColumn>& sourceColumns,
    const TMap<ui32, TTableInfo::TColumn>& alterColumns,
    const THashMap<TString, ui32>& colName2Id,
    const TSubDomainInfo& subDomain, TString& errStr)
{
    using TTtlProto = NKikimrSchemeOp::TTTLSettings;

    switch (ttl.GetStatusCase()) {
    case TTtlProto::kEnabled: {
        const auto& enabled = ttl.GetEnabled();
        const TString colName = enabled.GetColumnName();

        auto it = colName2Id.find(colName);
        if (it == colName2Id.end()) {
            errStr = Sprintf("Cannot enable TTL on unknown column: '%s'", colName.data());
            return false;
        }

        const TTableInfo::TColumn* column = nullptr;
        const ui32 colId = it->second;
        if (auto x = alterColumns.find(colId); x != alterColumns.end()) {
            column = &x->second;
        } else if (auto x = sourceColumns.find(colId); x != sourceColumns.end()) {
            column = &x->second;
        } else {
            Y_ABORT("Unknown column");
        }

        if (IsDropped(*column)) {
            errStr = Sprintf("Cannot enable TTL on dropped column: '%s'", colName.data());
            return false;
        }

        const auto unit = enabled.GetColumnUnit();
        if (!NValidation::TTTLValidator::ValidateUnit(GetType(*column), unit, errStr)) {
            return false;
        }

        if (!NValidation::TTTLValidator::ValidateTiers(enabled.GetTiers(), errStr)) {
            return false;
        }

        const auto expireAfter = GetExpireAfter(enabled, true);
        if (expireAfter.IsFail()) {
            errStr = expireAfter.GetErrorMessage();
            return false;
        }

        const TInstant now = TInstant::Now();
        if (expireAfter->Seconds() > now.Seconds()) {
            errStr = Sprintf("TTL should be less than %" PRIu64 " seconds (%" PRIu64 " days, %" PRIu64 " years). The ttl behaviour is undefined before 1970.", now.Seconds(), now.Days(), now.Days() / 365);
            return false;            
        }

        if (enabled.HasSysSettings()) {
            const auto& sys = enabled.GetSysSettings();
            if (TDuration::FromValue(sys.GetRunInterval()) < subDomain.GetTtlMinRunInterval()) {
                errStr = Sprintf("TTL run interval cannot be less than limit: %" PRIu64, subDomain.GetTtlMinRunInterval().Seconds());
                return false;
            }
        }
        break;
    }

    case TTtlProto::kDisabled:
        break;

    default:
        errStr = "TTL status must be specified";
        return false;
    }

    return true;
}

bool ValidateRowTtlExternalStorage(const NKikimrSchemeOp::TTTLSettings& ttl,
        TSchemeShard* schemeShard, TString& errStr) {
    if (!ttl.HasEnabled()) {
        return true;
    }

    const auto& tiers = ttl.GetEnabled().GetTiers();
    if (tiers.empty() || tiers.size() == 1 && tiers.Get(0).HasDelete()) {
        return true;
    }
    if (tiers.size() != 1 || !tiers.Get(0).HasEvictToExternalStorage()) {
        errStr = "Row-oriented TTL supports either DELETE or exactly one eviction tier";
        return false;
    }

    const TString& storagePath = tiers.Get(0).GetEvictToExternalStorage().GetStorage();
    TPath path = TPath::Resolve(storagePath, schemeShard);
    if (!path.IsResolved() || path.IsDeleted() || path.IsUnderDeleting()) {
        errStr = "TTL external data source not found: " + storagePath;
        return false;
    }
    if (!path->IsExternalDataSource()) {
        errStr = "TTL eviction target is not an external data source: " + storagePath;
        return false;
    }

    auto* source = schemeShard->ExternalDataSources.FindPtr(path->PathId);
    if (!source) {
        errStr = "Cannot resolve TTL external data source: " + storagePath;
        return false;
    }

    NKikimrSchemeOp::TExternalDataSourceDescription description;
    (*source)->FillProto(description, false);
    if (description.GetSourceType() != "ObjectStorage") {
        errStr = "Row-oriented TTL eviction supports only ObjectStorage external data sources";
        return false;
    }

    NColumnShard::NTiers::TTierConfig config;
    if (auto status = config.DeserializeFromProto(description); status.IsFail()) {
        errStr = "Cannot use external data source for row-oriented TTL eviction: " + status.GetErrorMessage();
        return false;
    }
    return true;
}

TConclusion<TDuration> GetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& settings, const bool allowNonDeleteTiers) {
    if (settings.TiersSize()) {
        const auto& tier = settings.GetTiers(0);
        if (tier.HasDelete() || allowNonDeleteTiers && tier.HasEvictToExternalStorage()) {
            return TDuration::Seconds(tier.GetApplyAfterSeconds());
        }
        return TConclusionStatus::Fail("Only DELETE or ObjectStorage eviction via TTL is allowed for row-oriented tables");
    } else {
        // legacy format
        return TDuration::Seconds(settings.GetExpireAfterSeconds());
    }
}

}}
