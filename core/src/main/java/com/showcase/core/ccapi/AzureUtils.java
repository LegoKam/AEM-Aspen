package com.showcase.core.ccapi;

import com.day.cq.dam.api.Asset;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.*;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;

import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Calendar;
import java.util.EnumSet;

public class AzureUtils {

    private String azureConnectionString;
    private String azureBlobContainer;
    private String containerRef;

    public AzureUtils(String azureConnectionString, String azureBlobContainer, String containerRef) {
        this.azureConnectionString = azureConnectionString;
        this.azureBlobContainer = azureBlobContainer;
        this.containerRef = containerRef;
    }

    public String prepareUploadBlob(ResourceResolver resourceResolver, String binaryFile) throws RepositoryException,
            URISyntaxException, IOException, InvalidKeyException, StorageException {
        if (binaryFile != null && resourceResolver != null) {
            Resource damResource = resourceResolver.getResource(binaryFile);
            if (damResource != null) {
                Asset asset = damResource.adaptTo(Asset.class);
                InputStream binaryStream = asset.getOriginal().getBinary().getStream();
                long size = asset.getOriginal().getBinary().getSize();

                String file = uploadAssetToAzure(asset.getName(), binaryStream, size);
                return file;
            }
        }
        return null;
    }

    public String uploadAssetToAzure(String assetName, InputStream binaryStream, long size) throws URISyntaxException,
            InvalidKeyException, StorageException, IOException {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerRef);
        CloudBlockBlob blob = container.getBlockBlobReference(assetName);
        blob.upload(binaryStream, size);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, 7);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(EnumSet.of(
                SharedAccessBlobPermissions.READ,
                SharedAccessBlobPermissions.WRITE,
                SharedAccessBlobPermissions.DELETE));
        policy.setSharedAccessExpiryTime(cal.getTime());

        String file = azureBlobContainer + assetName + "?" + blob.generateSharedAccessSignature(policy, null);


        return file;
    }

    public String uploadCSVBlob(String fileName, ByteArrayOutputStream csvContents)
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

    public String getSASURL(String fileName)
            throws InvalidKeyException, URISyntaxException, StorageException, IOException {
        CloudStorageAccount storageAccount = CloudStorageAccount.parse(azureConnectionString);
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerRef);
        CloudBlockBlob blob = container.getBlockBlobReference(fileName);
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.WRITE));
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR, 1);
        policy.setSharedAccessExpiryTime(cal.getTime());
        return azureBlobContainer + fileName + "?" + blob.generateSharedAccessSignature(policy, null);

    }

}
