package com.showcase.core.beans.photoshop.output;

import java.util.ArrayList;

public class Rendition{
    private String href;
    private String storage;
    private String type;
    private boolean trimToCanvas;
    public ArrayList<Layer> layers;


    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isTrimToCanvas() {
        return trimToCanvas;
    }

    public void setTrimToCanvas(boolean trimToCanvas) {
        this.trimToCanvas = trimToCanvas;
    }

    public ArrayList<Layer> getLayers() {
        return layers;
    }

    public void setLayers(ArrayList<Layer> layers) {
        this.layers = layers;
    }
}