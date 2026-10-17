class TagConstant:
    DATABASE_POSTGRESQL = "database:system:postgresql"
    DATABASE_KAFKA = "database:system:kafka"
    DATABASE_REDIS = "database:system:redis"
    DATABASE_MSSQL = "database:system:mssql"
    DATABASE_MONGODB = "database:system:mongodb"
    DATABASE_ORACLE = "database:system:oracle"
    DATABASE_DB2 = "database:system:db2"
    DATABASE_AS400 = "database:system:as400"
    FILE_FTP = "file:ftp"
    FILE_SFTP = "file:sftp"


class Owner:
    ASSISTED_CHANNEL_AND_MARKETING_PLATFORM = "assisted-channel-and-marketing-platform"
    BIG_DATA_PLATFORM = "big-data-platform"
    DATA_CENTER = "data-center"
    DATA_WAREHOUSE_HR_FINANCE_AND_REPORTING_PLATFORM = "data-warehouse-hr-finance-and-reporting-platform"
    DEVOPS = "devops"
    FUNDING_AND_CORE_BANKING_PLATFORM = "funding-and-core-banking-platform"
    IT_ARCHITECTURE = "it-architecture"
    IT_MANAGEMENT = "it-management"
    IT_SECURITY = "it-security"
    IT_SUPPORT = "it-support"
    LOAN_AND_CREDIT_PLATFORM = "loan-and-credit-platform"
    NETWORK = "network"
    OFFENSIVE_AND_RND_PLATFORM = "offensive-and-rnd-platform"
    ONBOARDING_API_AND_IDI_PLATFORM = "onboarding-api-and-idi-platform"
    OPERATION_INFRA = "operation_infra"
    PAYMENT_AND_COLLECTION_PLATFORM = "payment-and-collection-platform"
    PROPOSITION_O_AND_T_PLATFORM = "proposition-o-and-t-platform"
    SELF_SERVICE_CHANNEL_PLATFORM = "self-service-channel-platform"
    TREASURY_AUDIT_AND_CORAL_PLATFORM = "treasury-audit-and-coral-platform"
    VIRTUAL = "virtual"


class TimeEstimate:
    BELOW_1M = "estimate:time:1m"
    BETWEEN_1M_2M = "estimate:time:2m"
    BETWEEN_2M_5M = "estimate:time:5m"
    BETWEEN_5M_10M = "estimate:time:10m"
    BETWEEN_10M_60M = "estimate:time:60m"
    ABOVE_60M = "estimate:time:60m+"


class RecordEstimate:
    BELOW_1K = "estimate:record:1k"
    BETWEEN_1K_5K = "estimate:record:5k"
    BETWEEN_5K_10K = "estimate:record:10k"
    BETWEEN_10K_50K = "estimate:record:50k"
    BETWEEN_50K_100K = "estimate:record:100k"
    ABOVE_100K = "estimate:record:100k+"


class StorageEstimate:
    BELOW_1GB = "estimate:storage:1"
    BETWEEN_1GB_5GB = "estimate:storage:5"
    BETWEEN_5GB_10GB = "estimate:storage:10"
    BETWEEN_10GB_50GB = "estimate:storage:50"
    BETWEEN_50GB_100GB = "estimate:storage:100"
    ABOVE_100GB = "estimate:storage:100+"
