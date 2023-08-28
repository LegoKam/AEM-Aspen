package com.showcase.core.beans.ps.output;

import java.util.ArrayList;

public class Links {
    private ArrayList<Rendition> renditions;
    private Self self;

    public ArrayList<Rendition> getRenditions() {
        return renditions;
    }

    public void setRenditions(ArrayList<Rendition> renditions) {
        this.renditions = renditions;
    }

    public Self getSelf() {
        return self;
    }

    public void setSelf(Self self) {
        this.self = self;
    }
}