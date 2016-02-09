package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

    public SearchResult[] basicSearch(String query, int numResultsToSkip,
			int numResultsToReturn) {
        SearchEngine searchEngine = null;
        SearchResult[] searchResults = null;

		try {
            searchEngine = new SearchEngine("/var/lib/lucene/index", "searchField");
            TopDocs topDocs = searchEngine.performSearch(query,
                    numResultsToSkip + numResultsToReturn);
            ScoreDoc[] hits = topDocs.scoreDocs;
            int length = Math.min(hits.length, numResultsToSkip + numResultsToReturn);
            searchResults = new SearchResult[length - numResultsToSkip];

            for (int i = numResultsToSkip; i < length; i++) {
                Document doc = searchEngine.getDocument(hits[i].doc);
                String itemID = doc.get("ItemID");
                String name = doc.get("Name");

                searchResults[i - numResultsToSkip] = new SearchResult(itemID, name);
            }
        } catch(IOException ex) {
            System.out.println(ex);
        } catch(ParseException ex) {
            System.out.println(ex);
        }

        return searchResults;
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
        Connection conn = null;
        List<SearchResult> intersectResults = new ArrayList<SearchResult>();
        ResultSet spatialQueryResultSet = null;
        Set<String> spatialQuerySet = new HashSet<String>();

        // perform keyword based search
        SearchResult[] keywordQueryResult = basicSearch(query, 0, Integer.MAX_VALUE);

        // get DB connection, perform query
        try {
            conn = DbManager.getConnection(true);
            Statement stmt = conn.createStatement();
            String spatialQueryString = String.format("select ItemID from ItemLocationPoint " +
                    "where x(LocationPoint) between %s and %s and y(LocationPoint) between %s and %s",
                    region.getLx(), region.getRx(), region.getLy(), region.getRy());
            spatialQueryResultSet = stmt.executeQuery(spatialQueryString);

            while (spatialQueryResultSet.next()) {
                String itemID = spatialQueryResultSet.getString("ItemID");
                spatialQuerySet.add(itemID);
            }

            // close the database connection
            conn.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }

        for (SearchResult keywordEntry : keywordQueryResult) {
            if (spatialQuerySet.contains(keywordEntry.getItemId())) {
                intersectResults.add(keywordEntry);
            }
        }

        int toIndex = Math.min(numResultsToSkip + numResultsToReturn, intersectResults.size());
        intersectResults = intersectResults.subList(numResultsToSkip, toIndex);
        return intersectResults.toArray(new SearchResult[intersectResults.size()]);
	}

	public String getXMLDataForItemId(String itemId) {
        Connection conn = null;
        String xmlData = "";

        try {
            conn = DbManager.getConnection(true);
            Statement stmt = conn.createStatement();
            Map<String, String> itemsMap = getItemsData(itemId, stmt);
            if (itemsMap == null) {
                return "";
            }

            String name = itemsMap.get("Name");
            String location = itemsMap.get("Location");
            String country = itemsMap.get("Country");
            String userID = itemsMap.get("UserID");
            String description = itemsMap.get("Description");
            String started = itemsMap.get("Started");
            String ends = itemsMap.get("Ends");
            String currently = itemsMap.get("Currently");
            String buyPrice = itemsMap.get("BuyPrice");
            String firstBid = itemsMap.get("FirstBid");
            String numOfBids = itemsMap.get("NumberOfBids");

            ArrayList<String> categoryList = getCategoriesData(itemId, stmt);
            ArrayList<String> latLong = getLocationData(location, country, stmt);
            String sellerRating = getSellersData(userID, stmt);
            ArrayList<Map<String, String>> bidsList = getBidsData(itemId, stmt);

            // format into XML
            String categories, bids, locationAttributes;
            categories = bids = locationAttributes = "";
            categories = formatCategories(categoryList);
            xmlData = String.format(
                    "<Item ItemID=\"%s\">\n\t" +
                            "<Name>%s</Name>\n\t" +
                            "%s" +
                            "<Currently>%s</Currently>\n\t" +
                            "<Buy_Price>%s</Buy_Price>\n\t" +
                            "<First_Bid>%s</First_Bid>\n\t" +
                            "<Number_of_Bids>%s</Number_of_Bids>\n\t" +
                            "%s" +
                            "<Location%s>%s</Location>\n\t" +
                            "<Country>%s</Country>\n\t" +
                            "<Started>%s</Started>\n\t" +
                            "<Ends>%s</Ends>\n\t" +
                            "<Seller Rating=\"%s\" UserID=\"%s\" />\n\t" +
                            "<Description>%s</Description>\n\t" +
                            "</Item>",
                    itemId, name, categories, currently, buyPrice, firstBid,
                    numOfBids, bids, locationAttributes, location, country,
                    started, ends, sellerRating, userID, description
            );

        } catch (SQLException ex) {
            System.out.println(ex);
        }

        return xmlData;
	}

    private String formatCategories(ArrayList<String> categoryList) {
        String s = "";
        for (String category : categoryList) {
            s += String.format("<Category>%s</Category>\n\t", category);
        }
        return s;
    }

    private Map<String, String> getItemsData(String itemId, Statement stmt) throws SQLException {
        String name, location, country, userID, description, started, ends, currently, buyPrice, firstBid, numOfBids;
        String query = String.format("select * from Items where ItemID = %s", itemId);
        ResultSet resultSet = stmt.executeQuery(query);

        if (!resultSet.next()) {
            return null;
        }

        name = resultSet.getString("Name");
        location = resultSet.getString("Location");
        country = resultSet.getString("Country");
        userID = resultSet.getString("UserID");
        description = resultSet.getString("Description");

        SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
        started = outputFormat.format(resultSet.getTimestamp("Started"));
        ends = outputFormat.format(resultSet.getTimestamp("Ends"));

        currently = Float.toString(resultSet.getFloat("Currently"));
        buyPrice = Float.toString(resultSet.getFloat("Buy_Price"));
        firstBid = Float.toString(resultSet.getFloat("First_Bid"));
        numOfBids = Integer.toString(resultSet.getInt("Number_of_Bids"));

        Map<String, String> map = new HashMap<String, String>();
        map.put("Name", name);
        map.put("Location", location);
        map.put("Country", country);
        map.put("UserID", userID);
        map.put("Description", description);
        map.put("Started", started);
        map.put("Ends", ends);
        map.put("Currently", currently);
        map.put("BuyPrice", buyPrice);
        map.put("FirstBid", firstBid);
        map.put("NumberOfBids", numOfBids);

        return map;
    }

    private ArrayList<String> getCategoriesData(String itemId, Statement stmt) throws SQLException {
        String query = String.format("select Category from ItemCategory where ItemID = %s", itemId);
        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<String> list = new ArrayList<String>();

        while (resultSet.next()) {
            list.add(resultSet.getString("Category"));
        }

        return list;
    }

    private ArrayList<String> getLocationData(String location, String country, Statement stmt) throws SQLException {
        String query = String.format("select Latitude, Longitude from ItemLocationInfo " +
                "where Location = %s and Country = %s", location, country);
        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<String> list = new ArrayList<String>();

        while (resultSet.next()) {
            list.add(resultSet.getString("Latitude"));
            list.add(resultSet.getString("Longitude"));
        }

        return list;
    }

    private String getSellersData(String userId, Statement stmt) throws SQLException {
        String query = String.format("select Seller_Rating from Sellers where UserID = %s", userId);
        ResultSet resultSet = stmt.executeQuery(query);
        return Integer.toString(resultSet.getInt("Seller_Rating"));
    }

    private ArrayList<Map<String, String>> getBidsData(String itemId, Statement stmt) throws SQLException {
        String query = String.format("select * from Bids where ItemID = %s", itemId);
        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<Map<String, String>> bidsList = new ArrayList<Map<String, String>>();

        while (resultSet.next()) {
            Map<String, String> map = new HashMap<String, String>();
            String userId = resultSet.getString("UserID");
            map.put("UserID", userId);

            SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
            String time = outputFormat.format(resultSet.getTimestamp("Time"));
            map.put("Time", time);
            map.put("Amount", Float.toString(resultSet.getFloat("Amount")));
            map.putAll(getBiddersData(userId, stmt));
            bidsList.add(map);
        }

        return bidsList;
    }

    private Map<String, String> getBiddersData(String userId, Statement stmt) throws SQLException {
        String query = String.format("select * from Bidders where UserID = %s", userId);
        ResultSet resultSet = stmt.executeQuery(query);
        Map<String, String> bidderMap = new HashMap<String, String>();

        while (resultSet.next()) {
            bidderMap.put("BidderRating", Integer.toString(resultSet.getInt("Bidder_Rating")));
            bidderMap.put("Location", resultSet.getString("Location"));
            bidderMap.put("Country", resultSet.getString("Country"));
        }

        return bidderMap;
    }
	
	public String echo(String message) {
		return message;
	}

}
