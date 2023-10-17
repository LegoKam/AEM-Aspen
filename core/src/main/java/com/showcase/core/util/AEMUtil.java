package com.showcase.core.util;

import com.adobe.cq.dam.cfm.ContentFragment;
import com.adobe.granite.workflow.exec.WorkflowData;
import com.day.cq.dam.api.AssetManager;
import com.day.cq.tagging.Tag;
import com.day.cq.tagging.TagManager;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class AEMUtil {

    private static final Logger log = LoggerFactory.getLogger(AEMUtil.class);
    public static final String REQUEST_METHOD_GET = "GET";
    public static final String CONTENT_DAM = "/content/dam/";


    public static ContentFragment getContentFragment(String path, ResourceResolver resourceResolver) {
        Resource cfResource = resourceResolver.getResource(path);
        return Objects.requireNonNull(cfResource).adaptTo(ContentFragment.class);
    }

    public static String getPayload(WorkflowData workflowData) {
        String path = "";
        if ("JCR_PATH".equals(workflowData.getPayloadType())) {
            path = (String) workflowData.getPayload();
        } else {
            path = (String) workflowData.getPayload();
        }
        log.info("*****Indesign payload path*****" + path);
        return path;
    }

    public static ResourceResolver getResourceResolver(ResourceResolverFactory resourceResolverFactory)
            throws LoginException {
        Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "content-svc-admin");
        return resourceResolverFactory.getServiceResourceResolver(param);
    }

    public static void writeFinalAssetToDAM(ResourceResolver resourceResolver, String outputUrl, String path, String fileName) {

        try {
            int paramStart = outputUrl.indexOf("?");
            String baseUrl = null;
            String params = null;
            if (paramStart != -1) {
                baseUrl = outputUrl.substring(0, paramStart);
                params = outputUrl.substring(paramStart + 1);
            }

            log.info("baseUrl::" + baseUrl);

            String queryParams;
            String[] paramArray = Objects.requireNonNull(params).split("&");
            StringBuilder queryParamsBuilder = new StringBuilder("?");
            for (String parameter : paramArray) {
                String[] paramkv = parameter.split("=");
                queryParamsBuilder.append(paramkv[0]).append("=").append(URLEncoder.encode(paramkv[1], StandardCharsets.UTF_8));
            }
            queryParams = queryParamsBuilder.toString();

            log.info("queryParams::" + queryParams);

            log.info("Submit URL::" + baseUrl + queryParams);

            URL url = new URL(outputUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setConnectTimeout(59000);
            con.setReadTimeout(59000);

            con.setRequestMethod(REQUEST_METHOD_GET);

            con.connect();
            ValueFactory factory = Objects.requireNonNull(resourceResolver.adaptTo(Session.class)).getValueFactory();
            Binary binary = factory.createBinary(con.getInputStream());

            AssetManager assetManager = resourceResolver.adaptTo(AssetManager.class);
            String mimeType = "image/png";
            if (fileName.endsWith("psd")) {
                mimeType = "image/vnd.adobe.photoshop";
            }

            Objects.requireNonNull(assetManager).createOrReplaceAsset(path + fileName, binary, mimeType, true);

            log.info("Done!!");
        } catch (IOException | RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean isDAMAsset(String elementValue) {
        if (elementValue != null) {
            return elementValue.startsWith(CONTENT_DAM);
        }
        return false;
    }

    public static String getTagName(ResourceResolver resourceResolver, String tagName) {
        TagManager tagManager = resourceResolver.adaptTo(TagManager.class);
        Tag tag = Objects.requireNonNull(tagManager).resolve(tagName);
        return tag.getTitle();
    }

}