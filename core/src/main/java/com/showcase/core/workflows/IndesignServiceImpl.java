package com.showcase.core.workflows;

import com.adobe.cq.dam.cfm.ContentElement;
import com.adobe.cq.dam.cfm.ContentFragment;
import com.adobe.cq.dam.cfm.FragmentData;
import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowData;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.AssetManager;
import com.day.text.csv.Csv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import com.showcase.core.beans.Params;
import com.showcase.core.beans.Root;
import com.showcase.core.beans.Source;
import com.showcase.core.ccapi.CommonUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Binary;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.ValueFactory;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.stream.Collectors;

@Component(service = WorkflowProcess.class, property = {
        "process.label=Indesign Service Process"})
public class IndesignServiceImpl implements WorkflowProcess {

//  Workflow Process Arguments
//  azureBlobContainer = https://blobstorage4poc.blob.core.windows.net/container2/
//  azureConnectionString = DefaultEndpointsProtocol=https;AccountName=blobstorage4poc;AccountKey=EhYLOz7PjkIb7TZxY48hT/wAw1lD60fixxT1nT46u/Ey/siw/DYZIiepoSu2o30zonHgH/UjR5G0+AStArQU4g==;EndpointSuffix=core.windows.net
//  containerRef = container2
//  indesignAuthToken = Bearer eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2OTMxOTMxMDExODZfNzJjNmJiZjktNmVkZS00NjhmLWFhZjctZDc1NjUyMTdhM2M4X3V3MiIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiJjY2FzLXdlYl8wXzEiLCJ1c2VyX2lkIjoiQ0VDNUM3MDM1NDg5RjVBNjBBNEM5OEExQGFkb2JlLmNvbSIsInN0YXRlIjoie1wianNsaWJ2ZXJcIjpcInYyLXYwLjM4LjAtMTctZzYzMzMxOWRcIixcIm5vbmNlXCI6XCI5Njg1MTU1Mjc4MTQ1NzUwXCJ9IiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJDRUM1QzcwMzU0ODlGNUE2MEE0Qzk4QTFAYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IlhYTU0zSzdUWFBQNzRQNjJHT1FWM1hBQVBZPT09PT09Iiwic2lkIjoiMTY5MjgzODE5NDY2OF80OTc5ZTFmNC0yNDE1LTQzMGYtODI1MS0xNGUyYTlkZjdlZDJfdXcyIiwibW9pIjoiYTcyNTNhZDkiLCJwYmEiOiJNZWRTZWNOb0VWLExvd1NlYyIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoiQWRvYmVJRCxvcGVuaWQsY3JlYXRpdmVfY2xvdWQsaW5kZXNpZ25fc2VydmljZXMiLCJjcmVhdGVkX2F0IjoiMTY5MzE5MzEwMTE4NiJ9.JflXF3dkJCs6EPFzZfff8_cNFqwViXktLZRfxjmHK6TbW1-0rlm_oNpkDePbhufcfP2lrWjS7qHIGpaG5BFOM5odBbeHRUqyJ2-K5AO2vbwasj8wbjVpyxFCrvDTzhMQMGZWhXS8vvxnkCQ5zdqUZO9O6m7s-Hf7kHCsqaIacWXUUofjQSPx0NUqs9uJRzt564MOSU9FvLqbv8g65KrBksnHGF5AvqSiYNkPMnKzhuuFqyf-qhn0uw6bTL_3ihO7DE44TYUwrumheLbgzAn_c1DMdmsGIModcXX1_3IerWz1_4HMys-CEZt0YgA00Ihswdc8gyb6PPdU3elPo07Ang
//  indesignDataMergeServiceUrl = https://cxp.adobe.io/api/v1/capability/indesign/dataMerge/merge
//  indesignServiceAPIKey = ccas-web_0_1

    private static final Logger log = LoggerFactory.getLogger(IndesignServiceImpl.class);
    public static final String HTTP_GET = "HTTP_GET";
    public static final String HTTP_POST = "POST";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String REQUEST_METHOD_GET = "GET";
    public static final String CONTENT_DAM = "/content/dam/";
    private static final String PROCESS_ARGS = "PROCESS_ARGS";

    @Reference
    ResourceResolverFactory resourceResolverFactory;

    private String azureConnectionString;
    private String azureBlobContainer;
    private String containerRef;
    private String indesignAuthToken;
    private String indesignDataMergeServiceUrl;
    private String indesignServiceAPIKey;

    @Override
    public void execute(WorkItem item, WorkflowSession session, MetaDataMap map) throws WorkflowException {
        log.info("*****Indesign Services Start*****");

        try {

            initVariables(map);


            ResourceResolver resourceResolver = getResourceResolver();
            String payloadPath = getPayload(item.getWorkflowData());

            // Get the content fragment
            ContentFragment contentFragment = getContentFragment(payloadPath, resourceResolver);
            log.debug("Content Fragment:: " + contentFragment);

            // get content fragment elements
            Iterator<ContentElement> contentElements = contentFragment.getElements();

            String messageBody = prepareMessageBody(resourceResolver, contentElements);
            String messageResponse = invokeIndesignDataMergeService(messageBody);
            String nextCallUrl = lookupJson(messageResponse, "/statusUrls/all");

            String statusCallResponse = null;
            do {
                Thread.sleep(5 * 1000);
                statusCallResponse = invokeStatusCall(nextCallUrl);
                log.debug("STATUS RESPONSE:::: ?????" + statusCallResponse);
            } while (isRunning(statusCallResponse));

            statusCallResponse = invokeIndesignStatusCall(nextCallUrl);
            String outputPath = lookupJson(statusCallResponse, "/events/2/data/destination/url");
            log.debug("Final response>>>>>>>" + statusCallResponse);
            log.debug("Output Path response>>>>>>>" + outputPath);

            writeFinalAssetToDAM(resourceResolver, outputPath, payloadPath);


        } catch (LoginException | RepositoryException | URISyntaxException | IOException | InvalidKeyException |
                 StorageException | InterruptedException e) {
            log.error("LoginException::", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String invokeStatusCall(String call) throws Exception {

        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(REQUEST_METHOD_GET);
        con.setRequestProperty("accept", JSON_CONTENT_TYPE);
        con.setRequestProperty("x-api-key", indesignServiceAPIKey);
        con.setRequestProperty("Authorization", indesignAuthToken);
        con.connect();

        log.info("============response============");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    public static boolean isRunning(String statusCallResponse) throws JsonProcessingException {

        String status = CommonUtil.lookupJson(statusCallResponse, "/events/0/state");
        log.debug("STATUS??? ?????? " + status);
        if(status!=null && status.trim().length()==0) return true;
        return (!status.equals("COMPLETED"));
    }



    private void initVariables(MetaDataMap args) {
        String argumentsString = args.get(PROCESS_ARGS, "string");
        String[] argsArray = StringUtils.split(argumentsString, System.getProperty("line.separator"));
        Map<String, String> argsMap = Arrays.stream(argsArray)
                .map(String::trim)
                .map(s -> s.split(" = "))
                .filter(s -> s.length > 1)
                .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
        this.azureBlobContainer = argsMap.get("azureBlobContainer");
        this.azureConnectionString = argsMap.get("azureConnectionString");
        this.containerRef = argsMap.get("containerRef");
        this.indesignAuthToken = argsMap.get("indesignAuthToken");
        this.indesignDataMergeServiceUrl = argsMap.get("indesignDataMergeServiceUrl");
        this.indesignServiceAPIKey = argsMap.get("indesignServiceAPIKey");
        log.debug("\n--------------start - argsMap--------------" +
                "\nazureBlobContainer>>" + azureBlobContainer +
                "\nazureConnectionString>>" + azureConnectionString +
                "\ncontainerRef>>" + containerRef +
                "\nindesignAuthToken>>" + indesignAuthToken +
                "\nindesignDataMergeServiceUrl>>" + indesignDataMergeServiceUrl +
                "\nindesignServiceAPIKey>>" + indesignServiceAPIKey +
                "\n--------------end - argsMap--------------");
    }

    private void writeFinalAssetToDAM(ResourceResolver resourceResolver, String outputUrl, String path) throws Exception {

        int paramStart = outputUrl.indexOf("?");
        String baseUrl = null;
        if (paramStart != -1) {
            baseUrl = outputUrl.substring(0, paramStart);
        }

        log.info("baseUrl::" + baseUrl);

        URL url = new URL(outputUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(REQUEST_METHOD_GET);

        con.connect();
        ValueFactory factory = Objects.requireNonNull(resourceResolver.adaptTo(Session.class)).getValueFactory();
        Binary binary = factory.createBinary(con.getInputStream());

        AssetManager assetManager = resourceResolver.adaptTo(AssetManager.class);
        if (assetManager != null) {
            assetManager.createOrReplaceAsset(path + ".pdf", binary, "application/pdf", true);
        }

        log.info("Done!!");

    }

    private String lookupJson(String json, String path) throws JsonProcessingException {

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode rootNode = objectMapper.readTree(json);
        JsonNode val = rootNode.at(path);
        log.debug("JSON Lookup response::::" + val.asText());
        return val.asText();

    }

    private String prepareMessageBody(ResourceResolver resourceResolver, Iterator<ContentElement> contentElements) throws
            RepositoryException, URISyntaxException, IOException, InvalidKeyException, StorageException {

        String csvFileName = "filelist-" + Calendar.getInstance().getTimeInMillis() + ".csv";
        ByteArrayOutputStream csvByteOutputStream = new ByteArrayOutputStream();

        Root root = new Root();
        Params params = new Params();
        ArrayList<com.showcase.core.beans.Asset> assets = new ArrayList<>();
        root.setAssets(assets);
        root.setParams(params);

        params.setJobType("DATAMERGE_MERGE");
        params.setConvertUrlToHyperlink(false);
        params.setDataSource(csvFileName);

        Csv csv = new Csv();
        csv.setFieldDelimiter(',');
        csv.setLineSeparator("\r\n");
        ArrayList<String> header = new ArrayList<>();
        ArrayList<String> valueRow = new ArrayList<>();

        while (contentElements.hasNext()) {
            ContentElement contentElement = (ContentElement) contentElements.next();
            String elementName = contentElement.getName();
            FragmentData fragmentData = contentElement.getValue();
            String elementValue = (String) fragmentData.getValue();


            log.info("elementName--1::" + elementName);
            log.info("elementValue--1::" + elementValue);

            if (isDAMAsset(elementValue)) {
                com.showcase.core.beans.Asset damAsset = makeSourceObject(resourceResolver, elementValue);
                assets.add(damAsset);
                header.add("\"@" + elementName + "\"");
                valueRow.add("\"" + getAssetFileName(elementValue) + "\"");
            } else {
                header.add("\"" + elementName + "\"");
                valueRow.add("\"" + elementValue + "\"");
            }

            // check indesign template and output type
            if (elementName.toLowerCase().contains("indesign")) {
                params.setTargetDocument(getAssetFileName(elementValue));
            }
            if (elementName.toLowerCase().contains("outputtype")) {
                params.setOutputType(elementValue);
            }
        }
        csvByteOutputStream.write(String.join(",", header).getBytes(StandardCharsets.UTF_8));
        csvByteOutputStream.write(System.lineSeparator().getBytes(StandardCharsets.UTF_8));
        csvByteOutputStream.write(String.join(",", valueRow).getBytes(StandardCharsets.UTF_8));

        String csvFileURL = uploadCSVBlob(csvFileName, csvByteOutputStream);
        Source source = new Source();
        source.setUrl(csvFileURL);
        source.setType(HTTP_GET);
        com.showcase.core.beans.Asset csvAsset = new com.showcase.core.beans.Asset();
        csvAsset.setSource(source);
        csvAsset.setDestination(csvFileName);
        assets.add(csvAsset);

        Gson gson = new Gson();
        String json = gson.toJson(root);

        log.debug("JSON::::::::::" + json);
        return json;

    }

    private com.showcase.core.beans.Asset makeSourceObject(ResourceResolver resourceResolver, String binaryFile) {
        try {
            String blobUrl = prepareUploadBlob(resourceResolver, binaryFile);
            Source source = new Source();
            source.setUrl(blobUrl);
            source.setType(HTTP_GET);
            com.showcase.core.beans.Asset messageAsset = new com.showcase.core.beans.Asset();
            messageAsset.setSource(source);
            messageAsset.setDestination(getAssetFileName(binaryFile));
            return messageAsset;

        } catch (RepositoryException | URISyntaxException | IOException | InvalidKeyException | StorageException e) {
            throw new RuntimeException(e);
        }
    }

    private String invokeIndesignStatusCall(String call) throws Exception {

        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(REQUEST_METHOD_GET);
        con.setRequestProperty("accept", JSON_CONTENT_TYPE);
        con.setRequestProperty("x-api-key", indesignServiceAPIKey);
        con.setRequestProperty("Authorization", indesignAuthToken);
        con.connect();

        log.info("============response============");
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    private String invokeIndesignDataMergeService(String finalBody) throws IOException {

        log.debug("Service URL: " + indesignDataMergeServiceUrl);
        log.debug("Service API Key: " + indesignServiceAPIKey);
        log.debug("Service API Token: " + indesignAuthToken);
        log.debug("Service finalBody: " + finalBody);

        URL url = new URL(indesignDataMergeServiceUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(HTTP_POST);
        con.setRequestProperty("accept", JSON_CONTENT_TYPE);
        con.setRequestProperty("x-api-key", indesignServiceAPIKey);
        con.setRequestProperty("Authorization", indesignAuthToken);
        con.setRequestProperty("Content-Type", JSON_CONTENT_TYPE);
        con.setDoOutput(true);
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = finalBody.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
            os.close();
        }
        con.connect();
        log.debug("============response============");
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            return response.toString();
        }
    }

    private String getAssetFileName(String binaryFile) {
        return FilenameUtils.getName(binaryFile);
    }

    private String prepareUploadBlob(ResourceResolver resourceResolver, String binaryFile) throws RepositoryException,
            URISyntaxException, IOException, InvalidKeyException, StorageException {
        if (binaryFile != null && resourceResolver != null) {
            Resource damResource = resourceResolver.getResource(binaryFile);
            if (damResource != null) {
                Asset asset = damResource.adaptTo(Asset.class);
                InputStream binaryStream = null;
                if (asset != null) {
                    binaryStream = asset.getOriginal().getBinary().getStream();
                    long size = asset.getOriginal().getBinary().getSize();
                    return uploadAssetToAzure(asset.getName(), binaryStream, size);
                }
            }
        }
        return null;
    }

    private String uploadAssetToAzure(String assetName, InputStream binaryStream, long size) throws URISyntaxException,
            InvalidKeyException, StorageException, IOException {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerRef);
        CloudBlockBlob blob = container.getBlockBlobReference(assetName);
        blob.upload(binaryStream, size);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 7);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ));
        policy.setSharedAccessExpiryTime(cal.getTime());
        return azureBlobContainer + assetName + "?" + blob.generateSharedAccessSignature(policy, null);
    }

    private String uploadCSVBlob(String fileName, ByteArrayOutputStream csvContents)
            throws InvalidKeyException, URISyntaxException, StorageException, IOException {

        CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerRef);
        CloudBlockBlob blob = container.getBlockBlobReference(fileName);
        InputStream inputStream = new ByteArrayInputStream(csvContents.toByteArray());
        blob.upload(inputStream, csvContents.toByteArray().length);

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 7);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ));
        policy.setSharedAccessExpiryTime(cal.getTime());
        return azureBlobContainer + fileName + "?" + blob.generateSharedAccessSignature(policy, null);
    }

    private boolean isDAMAsset(String elementValue) {
        if (elementValue != null) {
            return elementValue.startsWith(CONTENT_DAM);
        }
        return false;
    }

    private ContentFragment getContentFragment(String path, ResourceResolver resourceResolver) {
        Resource cfResource = resourceResolver.getResource(path);
        return Objects.requireNonNull(cfResource).adaptTo(ContentFragment.class);
    }

    private String getPayload(WorkflowData workflowData) {
        String path = "";
        if ("JCR_PATH".equals(workflowData.getPayloadType())) {
            path = (String) workflowData.getPayload();
        } else {
            path = (String) workflowData.getPayload();
        }
        log.info("*****Indesign payload path*****" + path);
        return path;
    }


    private ResourceResolver getResourceResolver() throws LoginException {
        Map<String, Object> param = new HashMap<>();
        param.put(ResourceResolverFactory.SUBSERVICE, "content-svc-admin");
        return resourceResolverFactory.getServiceResourceResolver(param);
    }
}