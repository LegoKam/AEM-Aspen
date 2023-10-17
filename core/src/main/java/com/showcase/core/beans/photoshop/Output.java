package com.showcase.core.beans.photoshop;

import java.util.ArrayList;

public class Output{
    public String storage;
    public String type;
    public boolean overwrite;
    public String href;
    public boolean trimToCanvas;
    public ArrayList<Layer> layers;

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

    public boolean isOverwrite() {
        return overwrite;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public String getHref() {
        return href;
    }

    public void setHref(String href) {
        this.href = href;
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