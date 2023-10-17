package com.showcase.core.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.showcase.core.beans.photoshop.output.Links;
import com.showcase.core.beans.photoshop.output.Output;
import com.showcase.core.beans.photoshop.output.Rendition;
import com.showcase.core.beans.photoshop.output.Root;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class CommonUtil {

    private static final Logger log = LoggerFactory.getLogger(CommonUtil.class);
    public static final String CONTENT_DAM = "/content/dam/";


    public static String lookupJson(String json, String path) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(json);
        JsonNode val = rootNode.at(path);
        log.debug("JSON Lookup response::::" + val.asText());
        return val.asText();

    }

    public static ArrayList<String> getRenditions(String json) throws JsonProcessingException {

        log.debug("Renditions JSON::::: " + json);
        ArrayList<String> rendition2Download = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        Root outputRoot = objectMapper.readValue(json, Root.class);
        ArrayList<Output> outputArray = outputRoot.getOutputs();
        for (Output output : outputArray) {
            Links links = output.get_links();
            ArrayList<Rendition> renditionArray = links.getRenditions();
            for (Rendition rendition : renditionArray) {
                rendition2Download.add(rendition.getHref());
            }
        }
        return rendition2Download;
    }

    public static boolean isRunning(String statusCallResponse) throws JsonProcessingException {

        String status = lookupJson(statusCallResponse, "/outputs/0/status");
        return status.equals("running");
    }


}
