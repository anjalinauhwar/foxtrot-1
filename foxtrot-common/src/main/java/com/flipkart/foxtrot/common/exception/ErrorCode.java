package com.flipkart.foxtrot.common.exception;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
public enum ErrorCode {

    TABLE_INITIALIZATION_ERROR,
    TABLE_METADATA_FETCH_FAILURE,
    TABLE_NOT_FOUND,
    TABLE_ALREADY_EXISTS,
    TABLE_MAP_STORE_ERROR,
    INDEX_METADATA_STORE_EXCEPTION,
    INDEX_METADATA_EXCEPTION,


    TENANT_ALREADY_EXISTS,
    TENANT_NOT_FOUND,
    TENANT_NOT_CREATED,
    TENANT_MAP_STORE_ERROR,

    PIPELINE_ALREADY_EXISTS,
    PIPELINE_NOT_FOUND,
    PIPELINE_NOT_CREATED,
    PIPELINE_MAP_STORE_ERROR,

    INVALID_REQUEST,
    DOCUMENT_NOT_FOUND,
    MALFORMED_QUERY,
    CARDINALITY_OVERFLOW,

    ACTION_RESOLUTION_FAILURE,
    UNRESOLVABLE_OPERATION,
    ACTION_EXECUTION_ERROR,

    STORE_CONNECTION_ERROR,
    STORE_EXECUTION_ERROR,
    DATA_CLEANUP_ERROR,
    ELASTICSEARCH_QUERY_STORE_EXCEPTION,

    EXECUTION_EXCEPTION,
    CONSOLE_SAVE_EXCEPTION,
    CONSOLE_FETCH_EXCEPTION,

    AUTHORIZATION_EXCEPTION,

    SOURCE_MAP_CONVERSION_FAILURE,

    FQL_PERSISTENCE_EXCEPTION,
    FQL_PARSE_ERROR,

    PORT_EXTRACTION_ERROR,
    PERMISSION_CREATION_FAILURE,
    ENDPOINT_NOT_FOUND_ERROR,
    USER_NOT_FOUND,
    USER_PERMISSION_ADDITION_FAILURE,
    AUTH_TOKEN_EXCEPTION,

    CONSOLE_QUERY_BLOCKED,
    FQL_QUERY_BLOCKED,

    HBASE_REGIONS_EXTRACTION_FAILURE,
    HBASE_REGIONS_MERGE_FAILURE,

    DESERIALIZATION_ERROR,
    SERIALIZATION_ERROR,

    FUNNEL_EXCEPTION,
    CARDINALITY_CALCULATION_FAILURE,
    CARDINALITY_MAP_STORE_ERROR,

    NODE_GROUP_STORE_ERROR,
    NODE_GROUP_EXECUTION_ERROR

}