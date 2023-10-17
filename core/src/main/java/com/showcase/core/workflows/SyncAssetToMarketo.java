package com.showcase.core.workflows;

import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import com.day.cq.dam.api.Asset;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.showcase.core.beans.marketo.Root;
import com.showcase.core.util.AEMUtil;
import com.showcase.core.util.CommonUtil;
import com.showcase.core.util.HttpPostMultipart;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


@Component(service = WorkflowProcess.class, property = {"process.label=Sync Asset to Marketo"})
public class SyncAssetToMarketo implements WorkflowProcess {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Reference
    ResourceResolverFactory resourceResolverFactory;

    private static final String PROCESS_ARGS = "PROCESS_ARGS";

    private String clientId;
    private String clientSecret;
    private String marketoInstance;
    private String marketoFolder;
    private String assetRendition;

    // Config snapshot
    // clientId = 0c7ef3e7-6053-4ab8-801b-258b2852d1cc
    // clientSecret = J5dztVYPHk6QR81j0H5r0G6SzEkoah05
    // marketoFolder = {'id':34,'type':'Folder'}
    // marketoInstance = https://691-RNY-559.mktorest.com
    // assetRendition = cq5dam.web.1280.1280.jpeg

    private void initVariables(MetaDataMap args) {
        String argumentsString = args.get(PROCESS_ARGS, "string");
        String[] argsArray = StringUtils.split(argumentsString, System.getProperty("line.separator"));
        Map<String, String> argsMap = Arrays.stream(argsArray).map(String::trim).map(s -> s.split(" = ")).filter(s -> s.length > 1).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));

        this.clientId = argsMap.get("clientId");
        this.clientSecret = argsMap.get("clientSecret");
        this.marketoInstance = argsMap.get("marketoInstance");
        this.marketoFolder = argsMap.get("marketoFolder");
        this.assetRendition = argsMap.get("assetRendition");

        logger.debug("\n--------------start - argsMap--------------" + "\nclientId>>" + argsMap.get("clientId") + "\nclientSecret>>" + argsMap.get("clientSecret") + "\nmarketoInstance>>" + argsMap.get("marketoInstance") + "\n--------------end - argsMap--------------");
    }

    @Override
    public void execute(WorkItem item, WorkflowSession session, MetaDataMap map) throws WorkflowException {

        initVariables(map);
        String accessToken = null;

        try {

            // Get access token
            String accessTokenResponse = generateAccessToken();

            Gson gson = new GsonBuilder().disableHtmlEscaping().create();
            Root accessTokenObject = gson.fromJson(accessTokenResponse, Root.class);
            accessToken = accessTokenObject.getAccess_token();


            ResourceResolver resourceResolver = AEMUtil.getResourceResolver(resourceResolverFactory);
            String payloadPath = AEMUtil.getPayload(item.getWorkflowData());
            Asset asset = Objects.requireNonNull(resourceResolver
                    .getResource(payloadPath)).adaptTo(Asset.class);


            // Sync Asset to Marketo
            assetSync(accessToken, Objects.requireNonNull(asset));

        } catch (IOException e) {
            logger.error("Error", e);
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }

    }

    private void assetSync(String accessToken, Asset asset) throws IOException {
        String call = marketoInstance
                + "/rest/asset/v1/files.json?"
                + "access_token="
                + accessToken;

        Map<String, String> headers = new HashMap<>();
        HttpPostMultipart multipart = null;
        Resource assetResource = asset.getRendition(assetRendition);
        InputStream inputStream = assetResource.adaptTo(InputStream.class);

        multipart = new HttpPostMultipart(call, "utf-8", headers);
        // Add form field
        multipart.addFormField("folder", marketoFolder);
        multipart.addFormField("description", Objects.requireNonNull(asset, "").getMetadataValue("dc:title"));
        multipart.addFormField("name", "AEM " + asset.getName());
        // Add file
        multipart.addFilePart("file", new File(asset.getPath()), inputStream);
        // Print result
        String response = multipart.finish();
        logger.info("============Generate Asset Sync response============");
        logger.info(response);
    }

    private String generateAccessToken() throws IOException {

        String call = marketoInstance + "/identity/oauth/token"
                + "?grant_type=client_credentials"
                + "&client_id=" + clientId
                + "&client_secret=" + clientSecret;
        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.connect();
        logger.info("============Generate Token response============");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            logger.info(response.toString());
            return response.toString();
        }
    }

}
