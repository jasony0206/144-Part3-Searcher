package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

    private SearchEngine searchEngine = null;

    public SearchResult[] basicSearch(String queryString, int numResultsToSkip,
			int numResultsToReturn) {
        SearchResult[] searchResults = null;

		try {
            searchEngine = new SearchEngine("/var/lib/lucene/index", "searchField");
            TopDocs topDocs = searchEngine.performSearch(queryString,
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
		// TODO: Your code here!
		return new SearchResult[0];
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
