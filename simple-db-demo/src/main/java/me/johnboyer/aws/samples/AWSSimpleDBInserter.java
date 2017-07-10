/*
 * Copyright 2017 John Boyer. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package me.johnboyer.aws.samples;

import java.util.Arrays;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClientBuilder;
import com.amazonaws.services.simpledb.model.BatchPutAttributesRequest;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.CreateDomainResult;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataRequest;
import com.amazonaws.services.simpledb.model.DomainMetadataResult;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.ReplaceableItem;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.simpledb.model.SelectResult;

/*
 * Before running the code:
 *      Fill in your AWS access credentials in the provided credentials
 *      file template, and be sure to move the file to the default location
 *      (~/.aws/credentials) where the sample code will load the
 *      credentials from.
 *      https://console.aws.amazon.com/iam/home?#security_credential
 *
 * WARNING:
 *      To avoid accidental leakage of your credentials, DO NOT keep
 *      the credentials file in your source directory.
 */
/**
 * AWSSimpleDBInserter class
 * @author John Boyer
 *
 */
public class AWSSimpleDBInserter {
	/**
	 * Domain name
	 */
    private static final String DOMAIN_NAME = "customer";
    /**
     * Email attribute name
     */
    private static final String EMAIL_ATTR = "email";
    /**
     * First name attribute
     */
    private static final String FIRST_NAME_ATTR = "first_name";
    /**
     * Last name attribute
     */
	private static final String LAST_NAME_ATTR = "last_name";
	/**
	 * SimpleDB client
	 */
	private static AmazonSimpleDB sSDB;

  
	/**
	 * Creates the domain
	 * @return The domain result
	 */
	public static CreateDomainResult createDomain() {
		CreateDomainResult cdr = sSDB.createDomain(new CreateDomainRequest(DOMAIN_NAME));
		return cdr;
	}

    /**
	 * Creates the sample data
	 * @return The list of items
	 */
	public static List<ReplaceableItem> createSampleItems() {
		List<ReplaceableItem> items = Arrays.asList(
		        new ReplaceableItem("cust_001")
		            .withAttributes(new ReplaceableAttribute(FIRST_NAME_ATTR, "John", true), 
		                new ReplaceableAttribute(LAST_NAME_ATTR, "Doe", true),
		                new ReplaceableAttribute(EMAIL_ATTR, "john@example.com", true)),
		        new ReplaceableItem("cust_002")
		           .withAttributes(new ReplaceableAttribute(FIRST_NAME_ATTR, "Jane", true), 
		               new ReplaceableAttribute(LAST_NAME_ATTR, "Doe", true),
		                new ReplaceableAttribute(EMAIL_ATTR, "jane@example.com", true)),
		        new ReplaceableItem("cust_003")
		           .withAttributes(new ReplaceableAttribute(FIRST_NAME_ATTR, "Mary", true), 
		               new ReplaceableAttribute(LAST_NAME_ATTR, "Smith", true),
		                new ReplaceableAttribute(EMAIL_ATTR, "mary@example.com", true)),
		         new ReplaceableItem("cust_004")
		            .withAttributes(new ReplaceableAttribute(FIRST_NAME_ATTR, "Bob", true), 
		               new ReplaceableAttribute(LAST_NAME_ATTR, "Smith", true),
		                new ReplaceableAttribute(EMAIL_ATTR, "bob@example.com", true))     
		                                          );
		return items;
	}


	/**
	 * Deletes the domain
	 */
	public static void deleteDomain() {
		sSDB.deleteDomain(new DeleteDomainRequest(DOMAIN_NAME));
	}

	/**
	 * List the domains
	 */
	public static void listDomains() {
		ListDomainsRequest sdbRequest = new ListDomainsRequest().withMaxNumberOfDomains(100);
		ListDomainsResult sdbResult = sSDB.listDomains(sdbRequest);

		int totalItems = 0;
		for (String domainName : sdbResult.getDomainNames()) {
		    DomainMetadataRequest metadataRequest = new DomainMetadataRequest().withDomainName(domainName);
		    DomainMetadataResult domainMetadata = sSDB.domainMetadata(metadataRequest);
		    totalItems += domainMetadata.getItemCount();
		}
		
		System.out.println("You have " + sdbResult.getDomainNames().size() + " Amazon SimpleDB domain(s)" +
		        "containing a total of " + totalItems + " items.");
	}


	public static void main(String[] args) throws Exception {

        System.out.println("===========================================");
        System.out.println("AWS SimpleDB Demo app");
        System.out.println("===========================================");

        try {
        	sSDB = AmazonSimpleDBClientBuilder.defaultClient();
        	
        	CreateDomainResult cdr = createDomain();
        	System.out.println(cdr.toString());
        	
            listDomains();
            
            List<ReplaceableItem> items = createSampleItems();
            putAllItems(items);
            
            //Wait for the put to succeed
            Thread.sleep(1000);
            
            selectDomain();
            
            deleteDomain();
            
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

    }


	/**
	 * @param items
	 */
	public static void putAllItems(List<ReplaceableItem> items) {
		sSDB.batchPutAttributes(new BatchPutAttributesRequest(DOMAIN_NAME, items));
	}


	/**
	 * Selects and prints the domain
	 */
	public static void selectDomain() {
		SelectResult sr = sSDB.select(new SelectRequest("SELECT * FROM customer"));
		System.out.println(sr.toString());
	}
}
