#include "datashard_distributed_erase.h"
#include "datashard_impl.h"
#include "erase_rows_condition.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/wrappers/abstract.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/services/metadata/secret/accessor/secret_id.h>
#include <ydb/services/scheme_secret/resolver.h>

#include <contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3/include/aws/s3/model/PutObjectRequest.h>
#include <library/cpp/digest/md5/md5.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <yql/essentials/parser/pg_wrapper/postgresql/src/backend/catalog/pg_type_d.h>

namespace NKikimr {
namespace NDataShard {

using namespace NActors;
using namespace NTable;

using TLimits = NKikimrTxDataShard::TEvConditionalEraseRowsRequest::TLimits;
using TEvictionSettings = NKikimrTxDataShard::TEvConditionalEraseRowsRequest::TEvictionSettings;

struct TEvTtlSecretsResolved : TEventLocal<TEvTtlSecretsResolved, TEvents::ES_PRIVATE + 0x6f01> {
    NKqp::TEvDescribeSecretsResponse::TDescription Description;

    explicit TEvTtlSecretsResolved(NKqp::TEvDescribeSecretsResponse::TDescription description)
        : Description(std::move(description))
    {}
};

class IEraserOps {
protected:
    struct TKey {
        TTag Tag = Max<TTag>();
        TPos Pos = Max<TPos>();
    };

    virtual TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const = 0;
    virtual TActorId CreateEraser() = 0;
    virtual void CloseEraser() = 0;
};

class TCondEraseScan: public IActorCallback, public IActorExceptionHandler, public IScan, public IEraserOps {
    struct TDataShardId {
        TActorId ActorId;
        ui64 TabletId;
    };

    class TSerializedKeys {
    public:
        explicit TSerializedKeys(
                ui32 bytesLimit = 500 * 1024,
                ui32 minCount = 1000,
                ui32 maxCount = 50000)
            : BytesLimit(bytesLimit)
            , MinCount(minCount)
            , MaxCount(maxCount)
            , Size(0)
        {
        }

        void Add(TString key, TString row = {}) {
            Size += key.size() + row.size();
            Keys.emplace_back(std::move(key));
            Rows.emplace_back(std::move(row));
        }

        void Clear() {
            Keys.clear();
            Rows.clear();
            Size = 0;
        }

        TVector<TString>& GetKeys() {
            return Keys;
        }

        const TVector<TString>& GetRows() const {
            return Rows;
        }

        ui32 Count() const {
            return Keys.size();
        }

        ui32 Bytes() const {
            return Size;
        }

        bool CheckLimits() const {
            if (Size < BytesLimit) {
                if (Count() < MaxCount) {
                    return true;
                }
            } else {
                if (Count() < MinCount) {
                    return true;
                }
            }

            return false;
        }

        explicit operator bool() const {
            return Count();
        }

    private:
        const ui32 BytesLimit;
        const ui32 MinCount;
        const ui32 MaxCount;

        TVector<TString> Keys;
        TVector<TString> Rows;
        ui32 Size;
    };

    struct TStats {
        TStats()
            : RowsProcessed(0)
            , RowsErased(0)
        {
            auto counters = GetServiceCounters(AppData()->Counters, "tablets")->GetSubgroup("subsystem", "erase_rows");

            MonProcessed = counters->GetCounter("Processed", true);
            MonErased = counters->GetCounter("Erased", true);
        }

        void IncProcessed() {
            ++RowsProcessed;
            *MonProcessed += 1;
        }

        void IncErased(ui64 count) {
            RowsErased += count;
            *MonErased += count;
        }

        void ToProto(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::TStats& stats) const {
            stats.SetRowsProcessed(RowsProcessed);
            stats.SetRowsErased(RowsErased);
        }

    private:
        ui64 RowsProcessed;
        ui64 RowsErased;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonProcessed;
        ::NMonitoring::TDynamicCounters::TCounterPtr MonErased;
    };

    static TVector<TCell> MakeKeyCells(const TVector<TKey>& keyOrder, const TRow& row) {
        TVector<TCell> keyCells;

        for (const auto& key : keyOrder) {
            Y_ENSURE(key.Pos != Max<TPos>());
            Y_ENSURE(key.Pos < row.Size());
            keyCells.push_back(row.Get(key.Pos));
        }

        return keyCells;
    }

    static THolder<TEvDataShard::TEvEraseRowsRequest> MakeEraseRowsRequest(const TTableId& tableId,
            IEraseRowsCondition* condition, const TVector<TKey>& keyOrder, TVector<TString>& keys)
    {
        auto request = MakeHolder<TEvDataShard::TEvEraseRowsRequest>();

        request->Record.SetTableId(tableId.PathId.LocalPathId);
        request->Record.SetSchemaVersion(tableId.SchemaVersion);

        for (const auto& key : keyOrder) {
            Y_ENSURE(key.Tag != Max<TTag>());
            request->Record.AddKeyColumnIds(key.Tag);
        }

        for (TString& key : keys) {
            request->Record.AddKeyColumns(std::move(key));
        }

        condition->AddToRequest(request->Record);

        return request;
    }

    void SendEraseRowsRequest() {
        PendingEraseCount = SerializedKeys.Count();
        Send(CreateEraser(), MakeEraseRowsRequest(TableId, Condition.Get(), KeyOrder, SerializedKeys.GetKeys()));
        SerializedKeys.Clear();
    }

    void SendBatch() {
        if (!Eviction) {
            SendEraseRowsRequest();
            return;
        }

        NKikimrTxDataShard::TRowTtlEvictionBatch batch;
        batch.SetOwnerId(TableId.PathId.OwnerId);
        batch.SetTableId(TableId.PathId.LocalPathId);
        batch.SetSchemaVersion(TableId.SchemaVersion);
        batch.SetTabletId(DataShard.TabletId);
        for (const TTag tag : RowTags) {
            batch.AddColumnIds(tag);
        }
        for (const TString& row : SerializedKeys.GetRows()) {
            batch.AddRows(row);
        }

        TString body;
        Y_ENSURE(batch.SerializeToString(&body));
        const TString objectKey = TStringBuilder()
            << "ttl/" << TableId.PathId.OwnerId
            << "/" << TableId.PathId.LocalPathId
            << "/" << DataShard.TabletId
            << "/" << MD5::Calc(body) << ".pb";
        auto request = Aws::S3::Model::PutObjectRequest().WithKey(objectKey.c_str());
        Send(StorageWrapper, new NWrappers::TEvExternalStorage::TEvPutObjectRequest(request, std::move(body)));
    }

    void Reply(EStatus status = EStatus::Done) {
        auto response = MakeHolder<TEvDataShard::TEvConditionalEraseRowsResponse>();
        response->Record.SetTabletID(DataShard.TabletId);

        if (status != EStatus::Done) {
            response->Record.SetStatus(status == EStatus::Exception
                ? NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ERASE_ERROR
                : NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ABORTED);
        } else if (!Success) {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::ERASE_ERROR);
        } else if (!NoMoreData) {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::PARTIAL);
        } else {
            response->Record.SetStatus(NKikimrTxDataShard::TEvConditionalEraseRowsResponse::OK);
        }

        response->Record.SetErrorDescription(Error);
        Stats.ToProto(*response->Record.MutableStats());

        Send(ReplyTo, std::move(response));
    }

    void Handle(TEvDataShard::TEvEraseRowsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        Success = (record.GetStatus() == NKikimrTxDataShard::TEvEraseRowsResponse::OK);
        Error = record.GetErrorDescription();
        if (Success) {
            Stats.IncErased(PendingEraseCount);
        }
        PendingEraseCount = 0;

        CloseEraser();

        if (NoMoreData || !Success) {
            Driver->Touch(EScan::Final);
        } else {
            Reply();
            Driver->Touch(EScan::Feed);
        }
    }

    void Handle(TEvDataShard::TEvConditionalEraseRowsRequest::TPtr& ev) {
        ReplyTo = ev->Sender;
        Reply();
    }

    void Handle(NWrappers::TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        const auto& result = ev->Get()->Result;
        if (!result.IsSuccess()) {
            Success = false;
            Error = TStringBuilder() << "TTL ObjectStorage upload failed: " << result.GetError().GetMessage();
            SerializedKeys.Clear();
            Driver->Touch(EScan::Final);
            return;
        }
        SendEraseRowsRequest();
    }

    void Handle(TEvTtlSecretsResolved::TPtr& ev) {
        if (ev->Get()->Description.Status != Ydb::StatusIds::SUCCESS
            || ev->Get()->Description.SecretValues.size() != 2) {
            Success = false;
            if (ev->Get()->Description.Status == Ydb::StatusIds::SUCCESS) {
                Error = "TTL ObjectStorage secret resolver returned an unexpected number of values";
            } else {
                Error = TStringBuilder() << "TTL ObjectStorage secret resolution failed: "
                    << ev->Get()->Description.Issues.ToOneLineString();
            }
            Driver->Touch(EScan::Final);
            return;
        }

        auto settings = Eviction->GetObjectStorage();
        settings.SetAccessKey(ev->Get()->Description.SecretValues[0]);
        settings.SetSecretKey(ev->Get()->Description.SecretValues[1]);
        auto config = NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, settings);
        StorageWrapper = Register(NWrappers::CreateStorageWrapper(config->ConstructStorageOperator()));
        Driver->Touch(EScan::Feed);
    }

public:
    explicit TCondEraseScan(TDataShard* ds, const TActorId& replyTo,
        const TString& databaseName, const TTableId& tableId, ui64 txId,
        THolder<IEraseRowsCondition> condition, const TLimits& limits,
        std::optional<TEvictionSettings> eviction = std::nullopt
    )
        : IActorCallback(static_cast<TReceiveFunc>(&TCondEraseScan::StateWork), NKikimrServices::TActivity::CONDITIONAL_ERASE_ROWS_SCAN_ACTOR)
        , DatabaseName(databaseName)
        , TableId(tableId)
        , DataShard{ds->SelfId(), ds->TabletID()}
        , ReplyTo(replyTo)
        , TxId(txId)
        , Condition(std::move(condition))
        , Driver(nullptr)
        , SerializedKeys(limits.GetBatchMaxBytes(), limits.GetBatchMinKeys(), limits.GetBatchMaxKeys())
        , Eviction(std::move(eviction))
        , NoMoreData(false)
        , Success(true)
    {
    }

    void Describe(IOutputStream& o) const override {
        o << "CondEraseScan {"
          << " TableId: " << TableId
          << " TxId: " << TxId
        << " }";
    }

    IScan::TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme> scheme) override {
        TlsActivationContext->AsActorContext().RegisterWithSameMailbox(this);

        Driver = driver;
        Scheme = std::move(scheme);
        KeyOrder = MakeKeyOrder(Scheme); // fill tags

        // fill scan tags & positions in KeyOrder
        ScanTags = Condition->Tags();
        Y_ENSURE(ScanTags.size() == 1, "Multi-column conditions are not supported");

        THashMap<TTag, TPos> tagToPos;

        for (TPos pos = 0; pos < ScanTags.size(); ++pos) {
            Y_ENSURE(tagToPos.emplace(ScanTags.at(pos), pos).second);
        }

        for (auto& key : KeyOrder) {
            auto it = tagToPos.find(key.Tag);
            if (it == tagToPos.end()) {
                it = tagToPos.emplace(key.Tag, ScanTags.size()).first;
                ScanTags.push_back(key.Tag);
            }

            key.Pos = it->second;
        }

        if (Eviction) {
            for (const auto& col : Scheme->Cols) {
                if (!tagToPos.contains(col.Tag)) {
                    tagToPos.emplace(col.Tag, ScanTags.size());
                    ScanTags.push_back(col.Tag);
                }
                RowTags.push_back(col.Tag);
                RowPositions.push_back(tagToPos.at(col.Tag));
            }
        }

        Condition->Prepare(Scheme, 0);

        if (!Eviction) {
            return {EScan::Feed, {}};
        }

        const auto& settings = Eviction->GetObjectStorage();
        const auto accessKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(settings.GetAccessKey());
        const auto secretKey = NMetadata::NSecret::TSecretIdOrValue::DeserializeFromString(settings.GetSecretKey());
        Y_ENSURE(accessKey && secretKey, "Malformed ObjectStorage credential reference");

        TVector<TString> secretNames;
        for (const auto* value : {&*accessKey, &*secretKey}) {
            const TString serialized = value->SerializeToString();
            Y_ENSURE(serialized.StartsWith(NMetadata::NSecret::TSecretName::PrefixNoUser),
                "Row TTL ObjectStorage credentials must be schema secret names");
            secretNames.push_back(serialized.substr(NMetadata::NSecret::TSecretName::PrefixNoUser.size()));
        }

        auto promise = NThreading::NewPromise<NKqp::TEvDescribeSecretsResponse::TDescription>();
        auto future = promise.GetFuture();
        const TActorId selfId = SelfId();
        future.Subscribe([actorSystem = TlsActivationContext->ActorSystem(), selfId](const auto& result) {
            actorSystem->Send(selfId, new TEvTtlSecretsResolved(result.GetValue()));
        });
        Send(NSecret::MakeDescribeSchemaSecretServiceId(SelfId().NodeId()), new NSecret::TEvResolveSecret(
            MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{}),
            DatabaseName,
            std::move(secretNames),
            std::move(promise)));
        return {EScan::Sleep, {}};
    }

    void Registered(TActorSystem* sys, const TActorId&) override {
        sys->Send(DataShard.ActorId, new TDataShard::TEvPrivate::TEvConditionalEraseRowsRegistered(TxId, SelfId()));
    }

    EScan Seek(TLead& lead, ui64) override {
        lead.To(ScanTags, {}, ESeek::Lower);
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell>, const TRow& row) override {
        Stats.IncProcessed();
        if (!Condition->Check(row)) {
            return EScan::Feed;
        }

        TVector<TCell> rowCells;
        rowCells.reserve(RowPositions.size());
        for (const TPos pos : RowPositions) {
            rowCells.push_back(row.Get(pos));
        }
        SerializedKeys.Add(
            TSerializedCellVec::Serialize(MakeKeyCells(KeyOrder, row)),
            Eviction ? TSerializedCellVec::Serialize(rowCells) : TString());
        if (SerializedKeys.CheckLimits()) {
            return EScan::Feed;
        }

        SendBatch();
        return EScan::Sleep;
    }

    EScan Exhausted() override {
        NoMoreData = true;

        if (!SerializedKeys) {
            return EScan::Final;
        }

        SendBatch();
        return EScan::Sleep;
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        Reply(status);
        PassAway();

        return nullptr;
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void PassAway() override {
        CloseEraser();
        if (StorageWrapper) {
            Send(std::exchange(StorageWrapper, TActorId()), new TEvents::TEvPoisonPill());
        }
        IActor::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvEraseRowsResponse, Handle);
            hFunc(TEvDataShard::TEvConditionalEraseRowsRequest, Handle);
            hFunc(NWrappers::TEvExternalStorage::TEvPutObjectResponse, Handle);
            hFunc(TEvTtlSecretsResolved, Handle);
        }
    }

protected:
    TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const override {
        TVector<TKey> keyOrder;

        for (const auto& col : scheme->Cols) {
            if (!col.IsKey()) {
                continue;
            }

            if (keyOrder.size() < (col.Key + 1)) {
                keyOrder.resize(col.Key + 1);
            }

            keyOrder[col.Key].Tag = col.Tag;
        }

        return keyOrder;
    }

    TActorId CreateEraser() override {
        return DataShard.ActorId;
    }

    void CloseEraser() override {
        // nop
    }

protected:
    const TString DatabaseName;
    const TTableId TableId;

private:
    const TDataShardId DataShard;
    TActorId ReplyTo;
    const ui64 TxId;
    THolder<IEraseRowsCondition> Condition;

    IDriver* Driver;
    TIntrusiveConstPtr<TScheme> Scheme;
    TVector<TKey> KeyOrder;
    TVector<TTag> ScanTags;
    TVector<TTag> RowTags;
    TVector<TPos> RowPositions;
    TSerializedKeys SerializedKeys;
    std::optional<TEvictionSettings> Eviction;
    TActorId StorageWrapper;
    ui64 PendingEraseCount = 0;

    TStats Stats;
    bool NoMoreData;
    bool Success;
    TString Error;

}; // TCondEraseScan

class TIndexedCondEraseScan: public TCondEraseScan {
public:
    explicit TIndexedCondEraseScan(
            TDataShard* ds, const TActorId& replyTo,
            const TString& databaseName, const TTableId& tableId, ui64 txId,
            THolder<IEraseRowsCondition> condition, const TLimits& limits, TIndexes indexes,
            std::optional<TEvictionSettings> eviction)
        : TCondEraseScan(ds, replyTo, databaseName, tableId, txId, std::move(condition), limits, std::move(eviction))
        , Indexes(std::move(indexes))
    {
    }

protected:
    // Enrich key with indexed columns
    TVector<TKey> MakeKeyOrder(TIntrusiveConstPtr<IScan::TScheme> scheme) const override {
        auto keyOrder = TCondEraseScan::MakeKeyOrder(scheme);

        THashSet<TTag> keys;
        for (const auto& key : keyOrder) {
            keys.insert(key.Tag);
        }

        for (const auto& [_, keyMap] : Indexes) {
            for (const auto& [indexColumnId, mainColumnId] : keyMap) {
                Y_UNUSED(indexColumnId);

                if (keys.contains(mainColumnId)) {
                    continue;
                }

                const TColInfo* col = scheme->ColInfo(mainColumnId);
                Y_ENSURE(col);
                Y_ENSURE(col->Tag == mainColumnId);

                keyOrder.emplace_back().Tag = col->Tag;
                keys.insert(col->Tag);
            }
        }

        return keyOrder;
    }

    TActorId CreateEraser() override {
        Y_ENSURE(!Eraser);
        Eraser = this->Register(CreateDistributedEraser(this->SelfId(), DatabaseName, TableId, Indexes));
        return Eraser;
    }

    void CloseEraser() override {
        if (!Eraser) {
            return;
        }

        this->Send(std::exchange(Eraser, TActorId()), new TEvents::TEvPoisonPill());
    }

private:
    const TIndexes Indexes;

    TActorId Eraser;

}; // TIndexedCondEraseScan

IScan* CreateCondEraseScan(
        TDataShard* ds, const TActorId& replyTo, const TString& databaseName, const TTableId& tableId, ui64 txId,
        THolder<IEraseRowsCondition> condition, const TLimits& limits, TIndexes indexes,
        std::optional<TEvictionSettings> eviction)
{
    Y_ENSURE(ds);
    Y_ENSURE(condition.Get());

    if (!indexes) {
        return new TCondEraseScan(ds, replyTo, databaseName, tableId, txId, std::move(condition), limits, std::move(eviction));
    } else {
        return new TIndexedCondEraseScan(ds, replyTo, databaseName, tableId, txId, std::move(condition), limits,
            std::move(indexes), std::move(eviction));
    }
}

static TIndexes GetIndexes(const NKikimrTxDataShard::TEvConditionalEraseRowsRequest& record) {
    TIndexes result;
    if (!record.IndexesSize()) {
        return result;
    }

    for (const auto& index : record.GetIndexes()) {
        TKeyMap keyMap(Reserve(index.KeyMapSize()));
        for (const auto& kv : index.GetKeyMap()) {
            keyMap.emplace_back(kv.GetIndexColumnId(), kv.GetMainColumnId());
        }

        result.emplace(TTableId(index.GetOwnerId(), index.GetPathId(), index.GetSchemaVersion()), std::move(keyMap));
    }

    return result;
}

static bool CheckUnit(bool isDateType, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& error) {
    switch (unit) {
    case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
    case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
        if (isDateType) {
            error = "Unit cannot be specified for date type column";
            return false;
        } else {
            return true;
        }
    case NKikimrSchemeOp::TTTLSettings::UNIT_AUTO:
        if (isDateType) {
            return true;
        } else {
            error = "Unit should be specified for integral type column";
            return false;
        }
    default:
        error = TStringBuilder() << "Unknown unit: " << static_cast<ui32>(unit);
        return false;
    }
}

static bool CheckUnit(NScheme::TTypeInfo type, NKikimrSchemeOp::TTTLSettings::EUnit unit, TString& error) {
    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
        return CheckUnit(true, unit, error);

    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Uint64:
    case NScheme::NTypeIds::DyNumber:
        return CheckUnit(false, unit, error);

    case NScheme::NTypeIds::Pg:
        switch (NPg::PgTypeIdFromTypeDesc(type.GetPgTypeDesc())) {
            case DATEOID:
            case TIMESTAMPOID:
                return CheckUnit(true, unit, error);
            case INT4OID:
            case INT8OID:
                return CheckUnit(false, unit, error);
            default:
                error = "Unsupported PG type";
                return false;
        }
        break;

    default:
        error = TStringBuilder() << "Unsupported type: " << static_cast<ui32>(type.GetTypeId());
        return false;
    }
}

void TDataShard::Handle(TEvDataShard::TEvConditionalEraseRowsRequest::TPtr& ev, const TActorContext& ctx) {
    using TEvRequest = TEvDataShard::TEvConditionalEraseRowsRequest;
    using TEvResponse = TEvDataShard::TEvConditionalEraseRowsResponse;

    const auto& record = ev->Get()->Record;

    auto response = MakeHolder<TEvResponse>();
    response->Record.SetTabletID(TabletID());

    auto badRequest = [&record = response->Record](const TString& error) {
        record.SetStatus(TEvResponse::ProtoRecordType::BAD_REQUEST);
        record.SetErrorDescription(error);
    };

    const ui64 localPathId = record.GetTableId();
    if (GetUserTables().contains(localPathId)) {
        auto condition = record.GetConditionCase();

        if (condition && InFlightCondErase) {
            if (InFlightCondErase.Condition == condition && InFlightCondErase.IsActive()) {
                ctx.Send(ev->Forward(InFlightCondErase.ActorId));
            } else {
                if (InFlightCondErase.Condition != condition) {
                    response->Record.SetStatus(TEvResponse::ProtoRecordType::OVERLOADED);
                }

                ctx.Send(ev->Sender, std::move(response));
            }

            return;
        }

        TUserTable::TCPtr userTable = GetUserTables().at(localPathId);
        if (record.GetSchemaVersion() && userTable->GetTableSchemaVersion()
            && record.GetSchemaVersion() != userTable->GetTableSchemaVersion()) {

            response->Record.SetStatus(TEvResponse::ProtoRecordType::SCHEME_ERROR);
            response->Record.SetErrorDescription(TStringBuilder() << "Schema version mismatch"
                << ": got " << record.GetSchemaVersion()
                << ", expected " << userTable->GetTableSchemaVersion());
            ctx.Send(ev->Sender, std::move(response));
            return;
        }

        ui64 localTxId = 0;
        THolder<IScan> scan;

        switch (condition) {
            case TEvRequest::ProtoRecordType::kExpiration: {
                const ui32 columnId = record.GetExpiration().GetColumnId();
                auto column = userTable->Columns.find(columnId);

                if (column != userTable->Columns.end()) {
                    TString error;
                    if (CheckUnit(column->second.Type, record.GetExpiration().GetColumnUnit(), error)) {
                        localTxId = NextTieBreakerIndex++;
                        const auto tableId = TTableId(PathOwnerId, localPathId, record.GetSchemaVersion());
                        std::optional<TEvictionSettings> eviction;
                        if (record.HasEviction()) {
                            if (!record.GetEviction().HasObjectStorage()) {
                                badRequest("Row TTL eviction request does not contain ObjectStorage settings");
                                break;
                            }
                            eviction = record.GetEviction();
                        }
                        scan.Reset(CreateCondEraseScan(this, ev->Sender, record.GetDatabaseName(), tableId, localTxId,
                            THolder(CreateEraseRowsCondition(record)), record.GetLimits(), GetIndexes(record), std::move(eviction)));
                    } else {
                        badRequest(error);
                    }
                } else {
                    badRequest(TStringBuilder() << "Unknown column id: " << columnId);
                }
                break;
            }

            default:
                badRequest(TStringBuilder() << "Unknown condition: " << (ui32)condition);
                break;
        }

        if (scan) {
            const ui32 localTableId = userTable->LocalTid;
            Y_ENSURE(Executor()->Scheme().GetTableInfo(localTableId));

            auto* appData = AppData(ctx);
            const auto& taskName = appData->DataShardConfig.GetTtlTaskName();
            const auto taskPrio = appData->DataShardConfig.GetTtlTaskPriority();

            ui64 readAheadLo = appData->DataShardConfig.GetTtlReadAheadLo();
            if (ui64 readAheadLoOverride = GetTtlReadAheadLoOverride(); readAheadLoOverride > 0) {
                readAheadLo = readAheadLoOverride;
            }

            ui64 readAheadHi = appData->DataShardConfig.GetTtlReadAheadHi();
            if (ui64 readAheadHiOverride = GetTtlReadAheadHiOverride(); readAheadHiOverride > 0) {
                readAheadHi = readAheadHiOverride;
            }

            const ui64 scanId = QueueScan(localTableId, scan.Release(), localTxId,
                TScanOptions()
                    .SetResourceBroker(taskName, taskPrio)
                    .SetReadAhead(readAheadLo, readAheadHi)
                    .SetReadPrio(TScanOptions::EReadPrio::Low)
            );
            InFlightCondErase.Enqueue(localTxId, scanId, condition);
        }
    } else {
        badRequest(TStringBuilder() << "Unknown table id: " << localPathId);
    }

    ctx.Send(ev->Sender, std::move(response));
}

void TDataShard::Handle(TEvPrivate::TEvConditionalEraseRowsRegistered::TPtr& ev, const TActorContext& ctx) {
    if (!InFlightCondErase || InFlightCondErase.TxId != ev->Get()->TxId) {
        LOG_WARN_S(ctx, NKikimrServices::TX_DATASHARD, "Unknown conditional erase actor registered"
            << ": at: " << TabletID());
        return;
    }

    InFlightCondErase.ActorId = ev->Get()->ActorId;
}

} // NDataShard
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvEraseRowsResponse::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvEraseRowsResponse_EStatus_Name(value);
}

Y_DECLARE_OUT_SPEC(, NKikimrTxDataShard::TEvConditionalEraseRowsResponse::EStatus, stream, value) {
    stream << NKikimrTxDataShard::TEvConditionalEraseRowsResponse_EStatus_Name(value);
}
