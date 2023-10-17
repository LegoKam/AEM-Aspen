package com.showcase.core.beans.indesign;

public class Params {

    private String jobType;
    private String targetDocument;
    private String dataSource;
    private String outputType;
    private boolean convertUrlToHyperlink;

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getTargetDocument() {
        return targetDocument;
    }

    public void setTargetDocument(String targetDocument) {
        this.targetDocument = targetDocument;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getOutputType() {
        return outputType;
    }

    public void setOutputType(String outputType) {
        this.outputType = outputType;
    }

    public boolean isConvertUrlToHyperlink() {
        return convertUrlToHyperlink;
    }

    public void setConvertUrlToHyperlink(boolean convertUrlToHyperlink) {
        this.convertUrlToHyperlink = convertUrlToHyperlink;
    }
}