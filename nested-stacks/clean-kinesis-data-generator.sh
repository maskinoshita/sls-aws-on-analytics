#!/bin/bash

aws cognito-identity list-identity-pools --max-results 20 \
    | jq '.IdentityPools[] | select(.IdentityPoolName == "KinesisDataGeneratorUsers") | .IdentityPoolId' \
    | xargs -I {} aws cognito-identity delete-identity-pool --identity-pool-id {}

aws cognito-idp list-user-pools --max-results 20 \
    | jq '.UserPools[] | select(.Name == "Kinesis Data-Generator Users") | .Id' \
    | xargs -I {} aws cognito-idp delete-user-pool --user-pool-id {}
