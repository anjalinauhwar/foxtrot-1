package com.flipkart.foxtrot.core.config;

import com.flipkart.foxtrot.core.jobs.BaseJobConfig;

import javax.validation.constraints.Min;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@NoArgsConstructor
public class TableIndexMetadataJobConfig extends BaseJobConfig {

    private static final String JOB_NAME = "TableIndexMetadataJob";

    @Getter
    @Setter
    @Min(1)
    private int oldMetadataSyncDurationInDays = 14;

    @Override
    public String getJobName() {
        return JOB_NAME;
    }


}