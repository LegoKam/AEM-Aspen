package com.showcase.core.beans;

import java.util.ArrayList;

public class Root {
    private ArrayList<Asset> assets;
    private Params params;

    public Params getParams() {
        return params;
    }

    public void setParams(Params params) {
        this.params = params;
    }


    public ArrayList<Asset> getAssets() {
        return assets;
    }

    public void setAssets(ArrayList<Asset> assets) {
        this.assets = assets;
    }
}
