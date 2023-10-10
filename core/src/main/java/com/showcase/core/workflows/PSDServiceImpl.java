package com.showcase.core.workflows;


import com.adobe.cq.dam.cfm.ContentElement;
import com.adobe.cq.dam.cfm.ContentFragment;
import com.adobe.cq.dam.cfm.FragmentData;
import com.adobe.granite.workflow.WorkflowException;
import com.adobe.granite.workflow.WorkflowSession;
import com.adobe.granite.workflow.exec.WorkItem;
import com.adobe.granite.workflow.exec.WorkflowProcess;
import com.adobe.granite.workflow.metadata.MetaDataMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.storage.StorageException;
import com.showcase.core.beans.ps.*;
import com.showcase.core.ccapi.AEMUtil;
import com.showcase.core.ccapi.AzureUtils;
import com.showcase.core.ccapi.CommonUtil;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.RepositoryException;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.stream.Collectors;

@Component(service = WorkflowProcess.class, property = {"process.label=Photoshop Service Process"})
public class PSDServiceImpl implements WorkflowProcess {

//    Workflow process - arguments
//    azureBlobContainer = https://blobstorage4poc.blob.core.windows.net/container2/
//    azureConnectionString = DefaultEndpointsProtocol=https;AccountName=blobstorage4poc;AccountKey=bRvgwrBTEoBiFEJJiPaAGap/59q72P994/AzEKeu6gaqBzBEM4egds5Qv6fFx0YNOykWI3K34lki+AStmMmF2w==;EndpointSuffix=core.windows.net
//    containerRef = container2
//    psAuthToken = Bearer eyJhbGciOiJSUzI1NiIsIng1dSI6Imltc19uYTEta2V5LWF0LTEuY2VyIiwia2lkIjoiaW1zX25hMS1rZXktYXQtMSIsIml0dCI6ImF0In0.eyJpZCI6IjE2OTY5NDM5MDg5MDRfOTAxZThkYWUtMTZkOC00MTk3LTgzNDYtN2FhNDRjYzI0ZjhlX3V3MiIsInR5cGUiOiJhY2Nlc3NfdG9rZW4iLCJjbGllbnRfaWQiOiJjY2FzLXdlYl8wXzEiLCJ1c2VyX2lkIjoiQ0VDNUM3MDM1NDg5RjVBNjBBNEM5OEExQGFkb2JlLmNvbSIsInN0YXRlIjoie1wianNsaWJ2ZXJcIjpcInYyLXYwLjM4LjAtMTctZzYzMzMxOWRcIixcIm5vbmNlXCI6XCIzMjU0NTgxOTIzMzQ3MzQ5XCJ9IiwiYXMiOiJpbXMtbmExIiwiYWFfaWQiOiJDRUM1QzcwMzU0ODlGNUE2MEE0Qzk4QTFAYWRvYmUuY29tIiwiY3RwIjowLCJmZyI6IlgzR1A0STZFVlBQNU1IVU9HTVFWWVhBQVlRIiwic2lkIjoiMTY5Njk0MzkwODg5OV85ZTMxMWY4OC02MTVlLTRhZmItOWZlYS00ZDY4NzM0OTg1MDJfdXcyIiwibW9pIjoiNGNjNGZjNWUiLCJwYmEiOiJNZWRTZWNOb0VWLExvd1NlYyIsImV4cGlyZXNfaW4iOiI4NjQwMDAwMCIsInNjb3BlIjoiQWRvYmVJRCxvcGVuaWQsY3JlYXRpdmVfY2xvdWQsaW5kZXNpZ25fc2VydmljZXMiLCJjcmVhdGVkX2F0IjoiMTY5Njk0MzkwODkwNCJ9.Wm-UjeBDB7EgFgMwDCBuZPjSkAzzjt8BLP0IIn2_--vTo3RUpCh_lAZ1kIsfgwz05u-z0mL0YNsabOfQ81ErQBUjxiBHHTonphNPzVD1LFJ-rNMOCcZBDL42JjgiULncKUNTOR4sWlEnVj1sds7Uuf1dxC17vNxYeCe_4kaVkvj5cQhdyEg0umf_uZouqgtZHT_ws5d0NeC4D01R5WuzyIVs2iWhlTRsWL9oBKRFsR_BPh0JRttVu36xNUInfDxWkcSoJDa93OGVVQrzVCzFzlrdQxfN7c_XjDBfGSRekFoVcpXidRswYkNGLB8sYlcmI8i2uYFKPTnbzKXlIYMM3w
//    psDataMergeServiceUrl = https://image.adobe.io/pie/psdService/documentOperations
//    psServiceAPIKey = ccas-web_0_1
//    scene7Url = https://s7ap1.scene7.com/is/image/AGS489/



    private static final Logger log = LoggerFactory.getLogger(PSDServiceImpl.class);
    public static final String HTTP_GET = "HTTP_GET";
    public static final String HTTP_POST = "POST";
    public static final String JSON_CONTENT_TYPE = "application/json";
    public static final String REQUEST_METHOD_GET = "GET";
    private static final String PROCESS_ARGS = "PROCESS_ARGS";


    @Reference
    ResourceResolverFactory resourceResolverFactory;

    private String psAuthToken;
    private String psDataMergeServiceUrl;
    private String psServiceAPIKey;
    private AzureUtils azureUtils;
    private String scene7Url;

    private void initVariables(MetaDataMap args) {
        String argumentsString = args.get(PROCESS_ARGS, "string");
        String[] argsArray = StringUtils.split(argumentsString, System.getProperty("line.separator"));
        Map<String, String> argsMap = Arrays.stream(argsArray).map(String::trim).map(s -> s.split(" = ")).filter(s -> s.length > 1).collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));

        this.psAuthToken = argsMap.get("psAuthToken");
        this.psDataMergeServiceUrl = argsMap.get("psDataMergeServiceUrl");
        this.psServiceAPIKey = argsMap.get("psServiceAPIKey");

        this.scene7Url = argsMap.get("scene7Url");

        azureUtils = new AzureUtils(argsMap.get("azureConnectionString"), argsMap.get("azureBlobContainer"), argsMap.get("containerRef"));
        log.debug("\n--------------start - argsMap--------------" + "\nazureBlobContainer>>" + argsMap.get("azureBlobContainer") + "\nazureConnectionString>>" + argsMap.get("azureConnectionString") + "\ncontainerRef>>" + argsMap.get("containerRef") + "\npsAuthToken>>" + psAuthToken + "\npsDataMergeServiceUrl>>" + psDataMergeServiceUrl + "\npsServiceAPIKey>>" + psServiceAPIKey + "\n--------------end - argsMap--------------");
    }

    @Override
    public void execute(WorkItem item, WorkflowSession session, MetaDataMap map) throws WorkflowException {

        log.info("*****Photoshop Services Start*****");

        initVariables(map);

        try {
            ResourceResolver resourceResolver = AEMUtil.getResourceResolver(resourceResolverFactory);
            String payloadPath = AEMUtil.getPayload(item.getWorkflowData());

            // Get the content fragment
            ContentFragment contentFragment = AEMUtil.getContentFragment(payloadPath, resourceResolver);
            log.debug("Content Fragment:: " + contentFragment);

            // get content fragment elements
            Iterator<ContentElement> contentElements = contentFragment.getElements();

            Map<String, ContentElement> cfMap = loadMap(contentElements);
            String messageBody = prepareMessageBody(resourceResolver, cfMap);
            String messageResponse = invokePSDataMergeService(messageBody);


            String nextCallUrl = CommonUtil.lookupJson(messageResponse, "/_links/self/href");

            String statusCallResponse = null;
            do {
                Thread.sleep(5 * 1000);
                statusCallResponse = invokePSStatusCall(nextCallUrl);
            } while (CommonUtil.isRunning(statusCallResponse));


            ArrayList<String> downloadUrls = CommonUtil.getRenditions(statusCallResponse);
            log.debug("Final response>>>>>>>" + statusCallResponse);

            String outputLocation = Objects.requireNonNull(cfMap.get("outputRenditionsLocation").getValue().getValue(String.class)).trim();
            if (!outputLocation.endsWith("/")) {
                outputLocation = outputLocation + "/";
            }
            String outputFileName = cfMap.get("outputRenditionsFileName").getValue().getValue(String.class);
            for (String url : downloadUrls) {
                URL urlObj = new URL(url);
                String fileName = FilenameUtils.getName(urlObj.getPath());
                AEMUtil.writeFinalAssetToDAM(resourceResolver, url, outputLocation, outputFileName + "_" + fileName);
            }

        } catch (LoginException | RepositoryException | URISyntaxException | IOException | InvalidKeyException |
                 StorageException e) {
            log.error("Exception::", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    private String prepareMessageBody(ResourceResolver resourceResolver, Map<String, ContentElement> cfMap) throws RepositoryException, URISyntaxException, IOException, InvalidKeyException, StorageException {

        Root root = new Root();
        Edit editFlag = new Edit();
        Options options = new Options();

        ArrayList<Output> outputArray = new ArrayList<>();
        ArrayList<Layer> layerArrayList = new ArrayList<>();
        ArrayList<Input> inputArray = new ArrayList<>();

        Iterator<String> keyNames = cfMap.keySet().iterator();
        while (keyNames.hasNext()) {

            String elementName = keyNames.next();
            FragmentData fragmentData = cfMap.get(elementName).getValue();

            if (elementName.contains("Renditions") || fragmentData.getValue() == null) {
                log.debug("Skipping because its empty or element value is empty:----Element Name>>>" + elementName);
                continue;
            }
            String elementValue = fragmentData.getValue(String.class);
            log.debug("Progressing with ----" + elementName);
            log.debug("elementValue ----" + elementValue);

            //input
            if (elementName.toLowerCase().contains("psdtemplate")) {
                Input input = makeSourceObject(resourceResolver, elementValue);
                inputArray.add(input);
                log.debug("Input Array Ready ----" + inputArray);
                continue;
            }

            //options
            if (AEMUtil.isDAMAsset(elementValue)) {
                Input layerInput = makeSourceObject(resourceResolver, elementValue);
                if (cfMap.containsKey(elementName + "Renditions")) {
                    ContentElement contentElementRendition = cfMap.get(elementName + "Renditions");
                    String[] renditionArray = (String[]) contentElementRendition.getValue().getValue();
                    for (String rendition : Objects.requireNonNull(renditionArray)) {
                        String tagName = AEMUtil.getTagName(resourceResolver, rendition);
                        Layer layer = new Layer();
                        if (rendition.contains("smart-crop")) {
                            String smartCropUrl = this.scene7Url + FilenameUtils.getBaseName(elementValue) + ":" + tagName + "?fmt=png-alpha";
                            Input input = new Input();
                            input.setHref(smartCropUrl);
                            input.setStorage("external");
                            layer.setInput(input);
                        } else {
                            layer.setInput(layerInput);
                        }
                        layer.setName(elementName + "_" + tagName);
                        layer.setEdit(editFlag);
                        layerArrayList.add(layer);
                    }
                    log.debug("Layer Array Ready ----" + layerArrayList);
                }
                continue;
            } else {
                if (cfMap.containsKey(elementName + "Renditions")) {
                    ContentElement contentElementRendition = cfMap.get(elementName + "Renditions");
                    String[] renditionArray = (String[]) contentElementRendition.getValue().getValue();
                    for (String rendition : Objects.requireNonNull(renditionArray)) {
                        Layer textLayer = new Layer();
                        textLayer.setName(elementName + "_" + AEMUtil.getTagName(resourceResolver, rendition));
                        textLayer.setEdit(editFlag);
                        textLayer.setText(new Text(elementValue));
                        layerArrayList.add(textLayer);
                    }
                    log.debug("Text Array Ready ----" + layerArrayList);
                }
            }
        }

        String zeroByteString = "";

        if (cfMap.containsKey("outputRenditions")) {
            ContentElement contentElementRendition = cfMap.get("outputRenditions");
            String[] renditionArray = (String[]) contentElementRendition.getValue().getValue();
            for (String rendition : Objects.requireNonNull(renditionArray)) {
                Output imgOutput = new Output();
                String artBoardName = AEMUtil.getTagName(resourceResolver, rendition);

                InputStream emptyImgStream = new ByteArrayInputStream(zeroByteString.getBytes());
                String imgFileSASUrl = azureUtils.uploadAssetToAzure(artBoardName + ".png", emptyImgStream, zeroByteString.getBytes().length);

                imgOutput.setStorage("azure");
                imgOutput.setHref(imgFileSASUrl);
                imgOutput.setOverwrite(Boolean.TRUE);
                imgOutput.setType("image/png");
                imgOutput.setTrimToCanvas(true);

                Layer artBoardLayer = new Layer();
                artBoardLayer.setName(artBoardName);
                ArrayList<Layer> artBoardLayerList = new ArrayList<>();
                artBoardLayerList.add(artBoardLayer);
                imgOutput.setLayers(artBoardLayerList);

                outputArray.add(imgOutput);
            }
            log.debug("Output Array Ready ----" + outputArray);
        }

        //Create a dummy output file to be overwritten
        Output psdOutput = new Output();
        InputStream emptyStream = new ByteArrayInputStream(zeroByteString.getBytes());
        String outputFileSASUrl = azureUtils.uploadAssetToAzure("all_output_.psd", emptyStream, zeroByteString.getBytes().length);
        psdOutput.setStorage("azure");
        psdOutput.setHref(outputFileSASUrl);
        psdOutput.setOverwrite(Boolean.TRUE);
        psdOutput.setType("image/vnd.adobe.photoshop");
        outputArray.add(psdOutput);

        options.setLayers(layerArrayList);

        root.setInputs(inputArray);
        root.setOutputs(outputArray);
        root.setOptions(options);

        Gson gson = new GsonBuilder().disableHtmlEscaping().create();
        String json = gson.toJson(root);

        log.debug("JSON::::::::::" + json);
        return json;

    }

    private Map<String, ContentElement> loadMap(Iterator<ContentElement> contentElements) {

        Map<String, ContentElement> map = new HashMap<>();
        while (contentElements.hasNext()) {
            ContentElement contentElement = (ContentElement) contentElements.next();
            log.debug("Loading::" + contentElement.getName() + "-----" + contentElement);
            map.put(contentElement.getName(), contentElement);
        }
        return map;
    }


    private Input makeSourceObject(ResourceResolver resourceResolver, String binaryFile) {
        try {
            String blobUrl = azureUtils.prepareUploadBlob(resourceResolver, binaryFile);
            Input input = new Input();
            input.setHref(blobUrl);
            input.setStorage("external");
            return input;

        } catch (RepositoryException | URISyntaxException | IOException | InvalidKeyException | StorageException e) {
            throw new RuntimeException(e);
        }
    }

    private String invokePSStatusCall(String call) throws Exception {

        URL url = new URL(call);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(REQUEST_METHOD_GET);
        con.setRequestProperty("accept", JSON_CONTENT_TYPE);
        con.setRequestProperty("x-api-key", psServiceAPIKey);
        con.setRequestProperty("Authorization", psAuthToken);
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

    private String invokePSDataMergeService(String finalBody) throws IOException {

        log.debug("Service URL: " + psDataMergeServiceUrl);
        log.debug("Service API Key: " + psServiceAPIKey);
        log.debug("Service API Token: " + psAuthToken);
        log.debug("Service finalBody: " + finalBody);

        URL url = new URL(psDataMergeServiceUrl);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod(HTTP_POST);
        con.setRequestProperty("accept", JSON_CONTENT_TYPE);
        con.setRequestProperty("x-api-key", psServiceAPIKey);
        con.setRequestProperty("Authorization", psAuthToken);
        con.setRequestProperty("Content-Type", JSON_CONTENT_TYPE);
        con.setDoOutput(true);
        try (OutputStream os = con.getOutputStream()) {
            byte[] input = finalBody.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
            os.close();
        }
        con.connect();
        log.debug("============MAIN Response============");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder response = new StringBuilder();
            String responseLine = null;
            while ((responseLine = br.readLine()) != null) {
                response.append(responseLine.trim());
            }
            log.debug(response.toString());
            return response.toString();
        }
    }

}
