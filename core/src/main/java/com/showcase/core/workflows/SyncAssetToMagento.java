package com.showcase.core.workflows;

import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.commons.util.PrefixRenditionPicker;
import com.google.gson.Gson;
import com.showcase.core.beans.magento.Content;
import com.showcase.core.beans.magento.Entry;
import com.showcase.core.beans.magento.Root;
import com.showcase.core.beans.photoshop.Input;
import com.showcase.core.util.AEMUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.sling.api.resource.*;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


@Component(service = WorkflowProcess.class, property = {"process.label=Sync Asset to Magento/Commerce"})
public class SyncAssetToMagento implements WorkflowProcess {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Reference
    ResourceResolverFactory resourceResolverFactory;

    private static final String PROCESS_ARGS = "PROCESS_ARGS";

    private String username;
    private String password;
    private String magentoInstance;
    private String defaultSKU;
    private String assetRendition;

    // Config snapshot
    // username = Lpbmagentoadmin6
    // password = adobe@123
    // defaultSKU = {'id':34,'type':'Folder'}
    // magentoInstance = https://lpb-magento.adobedemo.com
    // assetRendition = cq5dam.web.1280.1280.png

    private void initVariables(MetaDataMap args) {
        String argumentsString = args.get(PROCESS_ARGS, "string");
        String[] argsArray = StringUtils.split(argumentsString, System.getProperty("line.separator"));
        Map<String, String> argsMap = Arrays.stream(argsArray).map(String::trim).map(s -> s.split(" = ")).filter(s -> s.length > 1).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));

        this.username = argsMap.get("username");
        this.password = argsMap.get("password");
        this.magentoInstance = argsMap.get("magentoInstance");
        this.defaultSKU = argsMap.get("defaultSKU");
        this.assetRendition = argsMap.get("assetRendition");

        logger.debug("--------------start - Magento - argsMap--------------");
        logger.debug("username>>" + argsMap.get("username"));
        logger.debug("password>>" + argsMap.get("password"));
        logger.debug("magentoInstance>>" + argsMap.get("magentoInstance"));
        logger.debug("defaultSKU>>" + argsMap.get("defaultSKU"));
        logger.debug("assetRendition>>" + argsMap.get("assetRendition"));
        logger.debug("--------------end - Magento - argsMap--------------");
    }

    @Override
    public void execute(WorkItem item, WorkflowSession session, MetaDataMap map) throws WorkflowException {

        initVariables(map);
        String accessToken = null;

        try {
            // Get access token
            accessToken = generateAccessToken();

            ResourceResolver resourceResolver = AEMUtil.getResourceResolver(resourceResolverFactory);
            String payloadPath = AEMUtil.getPayload(item.getWorkflowData());
            Asset asset = Objects.requireNonNull(resourceResolver.getResource(payloadPath)).adaptTo(Asset.class);

            accessToken = accessToken.replace("\"", "");


            // Sync Asset to Marketo
            String commerceResponse = assetSync(accessToken, Objects.requireNonNull(asset), resourceResolver);

            if (NumberUtils.isParsable(commerceResponse)) {
                Resource metadataRes = Objects.requireNonNull(asset.adaptTo(Resource.class)).getChild("jcr:content/metadata");
                ModifiableValueMap metadataMap = Objects.requireNonNull(metadataRes).adaptTo(ModifiableValueMap.class);
                Objects.requireNonNull(metadataMap).put("magentoAssetID", commerceResponse);
                resourceResolver.commit();
            }

        } catch (LoginException | RepositoryException | IOException e) {
            logger.debug("Error", e);
        }

    }

    private String assetSync(String accessToken, Asset asset, ResourceResolver resourceResolver) throws IOException, RepositoryException {

        String sku = asset.getMetadataValue("cq:products");

        String bearerToken = "Bearer " + accessToken;
        String call = magentoInstance + "/rest/default/V1/products/" + sku + "/media";

        Gson gson = new Gson();

        InputStream inputStream = asset.getRendition(new PrefixRenditionPicker(assetRendition, true)).getStream();
        byte[] byteArray = IOUtils.toByteArray(inputStream);


        //Root Object
        Root root = new Root();

        // Entry Object
        Entry entry = new Entry();
        Resource metadataResource = resourceResolver.getResource(asset.getPath() + "/jcr:content/metadata");
        Node metadataNode = Objects.requireNonNull(metadataResource).adaptTo(Node.class);
        String magentoAssetID = null;
        if (Objects.requireNonNull(metadataNode).hasProperty("magentoAssetID")) {
            magentoAssetID = metadataNode.getProperty("magentoAssetID").getString();
            if (magentoAssetID != null && magentoAssetID.trim().length() > 0) {
                entry.setId(Integer.parseInt(magentoAssetID));
                call = call + "/" + magentoAssetID;
            }
        }
        entry.setLabel(Objects.requireNonNull(asset, "").getMetadataValue("dc:title"));

        entry.setMediaType("image");
        entry.setPosition(0);
        ArrayList<String> types = new ArrayList<>();
        types.add("image");
        entry.setTypes(types);

        // Content Object
        Content content = new Content();
        content.setName(asset.getName());
        content.setType(asset.getMimeType());


        entry.setContent(content);
        root.setEntry(entry);

        logger.debug("DATA JSON before CONTENT>>>>>> " + gson.toJson(root));

        String encodedString = Base64.getEncoder().encodeToString(byteArray);
        content.setBase64_encoded_data(encodedString);

        String dataJson = gson.toJson(root);


        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestProperty("Content-Type", "application/json");
        con.setRequestProperty("Authorization", bearerToken);
        if (magentoAssetID != null && magentoAssetID.trim().length() > 0) {
            con.setRequestMethod("PUT");
            logger.debug("This is an Commerce Asset Update request.");
        }

        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(dataJson);
        wr.flush();
        wr.close();
        con.connect();

        logger.info("============Generate Token Magento response============");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            logger.info(response.toString());
            return response.toString().replace("\"", "");
        }

    }

    private String generateAccessToken() throws IOException {

        logger.debug("*******invoking generate Magento access token **********");
        String call = magentoInstance + "/rest/default/V1/integration/admin/token";
        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestProperty("Content-Type", "application/json");
        String urlParameters = "{\"username\":\"" + username + "\", \"password\":\"" + password + "\"}";
        con.setDoOutput(true);
        DataOutputStream wr = new DataOutputStream(con.getOutputStream());
        wr.writeBytes(urlParameters);
        wr.flush();
        wr.close();
        con.connect();

        logger.info("============Generate Token Magento response============");
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
