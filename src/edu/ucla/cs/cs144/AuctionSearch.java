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
            categories = formatCategories(categoryList);
            bids = formatBids(bidsList);
            locationAttributes = formatLocationAttributes(latLong);
            buyPrice = buyPrice.equals("0.00")? "" : "<Buy_Price>$" + buyPrice + "</Buy_Price>\n";

            xmlData = String.format(
                            "<Item ItemID=\"%s\">\n" +
                            "<Name>%s</Name>\n" +
                            "%s" +
                            "<Currently>$%s</Currently>\n" +
                            "%s" +
                            "<First_Bid>$%s</First_Bid>\n" +
                            "<Number_of_Bids>%s</Number_of_Bids>\n" +
                            "<Bids>\n%s</Bids>\n" +
                            "<Location%s>%s</Location>\n" +
                            "<Country>%s</Country>\n" +
                            "<Started>%s</Started>\n" +
                            "<Ends>%s</Ends>\n" +
                            "<Seller Rating=\"%s\" UserID=\"%s\" />\n" +
                            "<Description>%s</Description>\n" +
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
            s += String.format("<Category>%s</Category>\n", category);
        }
        return s;
    }

    private String formatBids(ArrayList<Map<String, String>> bidsList) {
        String s = "";
        for (Map<String, String> map : bidsList) {
            String locationCountry = "";
            String location = map.get("Location");
            String country = map.get("Country");
            if (location != null) {
                locationCountry += String.format("<Location>%s</Location>\n", location);
            }
            if (country != null) {
                locationCountry += String.format("<Country>%s</Country>\n", country);
            }
            s += String.format(
                            "<Bid>\n<Bidder Rating=\"%s\" UserID=\"%s\">\n" +
                            "%s" +
                            "</Bidder>\n" +
                            "<Time>%s</Time>\n" +
                            "<Amount>$%s</Amount>\n" +
                            "</Bid>\n",
                        map.get("BidderRating"), map.get("UserID"), locationCountry,
                        map.get("Time"), map.get("Amount")
            );
        }
        return s;
    }

    private String formatLocationAttributes(ArrayList<String> latLong) {
        String s = "";
        String latitude = latLong.get(0);
        String longitude = latLong.get(1);
        if (latitude != null && longitude != null) {
            s = String.format(" Latitude=\"%s\" Longitude=\"%s\"",
                    latitude, longitude);
        }
        return s;
    }

    private Map<String, String> getItemsData(String itemId, Statement stmt) throws SQLException {
        String name, location, country, userID, description, started, ends, currently, buyPrice, firstBid, numOfBids;
        String query = String.format("select * from Items where ItemID = %s", itemId);
        System.out.println(query);
        ResultSet resultSet = stmt.executeQuery(query);

        if (!resultSet.next()) {
            return null;
        }

        name = escapeChars(resultSet.getString("Name"));
        location = escapeChars(resultSet.getString("Location"));
        country = escapeChars(resultSet.getString("Country"));
        userID = escapeChars(resultSet.getString("UserID"));
        description = escapeChars(resultSet.getString("Description"));

        SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
        started = outputFormat.format(resultSet.getTimestamp("Started"));
        ends = outputFormat.format(resultSet.getTimestamp("Ends"));

        currently = String.format("%.2f", resultSet.getFloat("Currently"));
        buyPrice = String.format("%.2f", resultSet.getFloat("Buy_Price"));
        firstBid = String.format("%.2f", resultSet.getFloat("First_Bid"));
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
        System.out.println(query);

        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<String> list = new ArrayList<String>();

        while (resultSet.next()) {
            list.add(escapeChars(resultSet.getString("Category")));
        }

        return list;
    }

    private ArrayList<String> getLocationData(String location, String country, Statement stmt) throws SQLException {
        String query = String.format("select Latitude, Longitude from LocationInfo " +
                "where Location = \"%s\" and Country = \"%s\"", location, country);
        System.out.println(query);

        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<String> list = new ArrayList<String>();

        while (resultSet.next()) {
            list.add(resultSet.getString("Latitude"));
            list.add(resultSet.getString("Longitude"));
        }

        return list;
    }

    private String getSellersData(String userId, Statement stmt) throws SQLException {
        String query = String.format("select SellerRating from Sellers where UserID = \"%s\"", userId);
        System.out.println(query);
        Integer rating = 0;
        ResultSet resultSet = stmt.executeQuery(query);
        while (resultSet.next()) {
            rating = resultSet.getInt("SellerRating");
        }
        return Integer.toString(rating);
    }

    private ArrayList<Map<String, String>> getBidsData(String itemId, Statement stmt) throws SQLException {
        String query = String.format("select * from Bids where ItemID = %s", itemId);
        System.out.println(query);

        ResultSet resultSet = stmt.executeQuery(query);
        ArrayList<Map<String, String>> bidsList = new ArrayList<Map<String, String>>();

        while (resultSet.next()) {
            Map<String, String> map = new HashMap<String, String>();
            String userId = escapeChars(resultSet.getString("UserID"));
            map.put("UserID", userId);

            SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
            String time = outputFormat.format(resultSet.getTimestamp("Time"));
            map.put("Time", time);
            map.put("Amount", String.format("%.2f", resultSet.getFloat("Amount")));
            bidsList.add(map);
        }

        for (Map<String, String> bid : bidsList) {
            String userId = bid.get("UserID");
            bid.putAll(getBiddersData(userId, stmt));
        }

        return bidsList;
    }

    private Map<String, String> getBiddersData(String userId, Statement stmt) throws SQLException {
        String query = String.format("select * from Bidders where UserID = \"%s\"", userId);
        System.out.println(query);

        ResultSet resultSet = stmt.executeQuery(query);
        Map<String, String> bidderMap = new HashMap<String, String>();

        while (resultSet.next()) {
            bidderMap.put("BidderRating", Integer.toString(resultSet.getInt("BidderRating")));
            bidderMap.put("Location", escapeChars(resultSet.getString("Location")));
            bidderMap.put("Country", escapeChars(resultSet.getString("Country")));
        }

        return bidderMap;
    }

    private String escapeChars(String s) {
        s = s.replace("&", "&amp;");
//            .replace("\"", "&quot;")
//            .replace("'", "&apos;")
//            .replace("<", "&lt;")
//            .replace(">", "&gt;");

        return s;
    }
	
	public String echo(String message) {
		return message;
	}

}
