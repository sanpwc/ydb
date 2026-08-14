#pragma once

#include <ydb/core/base/feature_flags.h>
#include <ydb/core/kqp/common/events/script_executions.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor.h>

#include <library/cpp/retry/retry_policy.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NSecret {

inline NActors::TActorId MakeDescribeSchemaSecretServiceId(ui32 nodeId) {
    const char name[12] = "kqp_dsc_sec";
    return NActors::TActorId(nodeId, TStringBuf(name, 12));
}

struct TDescribeSecretSettings {
    IRetryPolicy<>::TPtr RetryPolicy;
};

struct TEvResolveSecret : public NActors::TEventLocal<TEvResolveSecret,
        EventSpaceBegin(TKikimrEvents::ES_PRIVATE)> {
    TEvResolveSecret(
        TIntrusiveConstPtr<NACLib::TUserToken> userToken,
        TString database,
        TVector<TString> secretNames,
        NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> promise,
        TDescribeSecretSettings settings = {})
        : UserToken(std::move(userToken))
        , Database(std::move(database))
        , SecretNames(std::move(secretNames))
        , Promise(std::move(promise))
        , Settings(std::move(settings))
    {
        Y_ENSURE(!Database.empty(), "Database name must be set in secret requests");
    }

    THolder<TEvResolveSecret> MakeCopy() const {
        return MakeHolder<TEvResolveSecret>(UserToken, Database, SecretNames, Promise, Settings);
    }

    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
    const TVector<TString> SecretNames;
    NThreading::TPromise<NKqp::TEvDescribeSecretsResponse::TDescription> Promise;
    const TDescribeSecretSettings Settings;
};

IRetryPolicy<>::TPtr MakeShortRetryPolicy();
IRetryPolicy<>::TPtr MakeLongRetryPolicy();

NThreading::TFuture<NKqp::TEvDescribeSecretsResponse::TDescription> DescribeSecret(
    const TVector<TString>& secretNames,
    const TIntrusiveConstPtr<NACLib::TUserToken> userToken,
    const TString& database,
    NActors::TActorSystem* actorSystem,
    TDescribeSecretSettings settings = {}
);

bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TVector<TString>& secretNames);
bool UseSchemaSecrets(const NKikimr::TFeatureFlags& flags, const TString& secretName);

}  // namespace NKikimr::NSecret
