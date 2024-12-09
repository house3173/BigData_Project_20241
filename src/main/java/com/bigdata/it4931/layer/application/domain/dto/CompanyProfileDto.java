package com.bigdata.it4931.layer.application.domain.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class CompanyProfileDto {
    @JsonProperty("Sector")
    private String sector;

    @JsonProperty("Industry")
    private String industry;

    @JsonProperty("City")
    private String city;

    @JsonProperty("State")
    private String state;

    @JsonProperty("Zip")
    private String zip;

    @JsonProperty("Website")
    private String website;

    @JsonProperty("Ticker")
    private String ticker;

    @JsonProperty("CEO")
    private String ceo;
}

