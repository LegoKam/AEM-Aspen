package com.showcase.core.beans.magento;

public class Content {
    private String base64_encoded_data;
    private String type;
    private String name;

    public String getBase64_encoded_data() {
        return base64_encoded_data;
    }

    public void setBase64_encoded_data(String base64_encoded_data) {
        this.base64_encoded_data = base64_encoded_data;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}