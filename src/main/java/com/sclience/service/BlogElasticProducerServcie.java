package com.sclience.service;


import com.iscliecne.elastic.IBlogElasticService;
import com.iscliecne.entity.Blog;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BlogElasticProducerServcie implements IBlogElasticService {

    public String addIndex(Blog blog) {
        try {
            TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("212.64.29.78"), 9300));
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder().startObject()
                    .field("id", blog.getId())
                    .field("title", blog.getTitle())
                    .field("releaseDate", blog.getReleaseDateStr())
                    .field("content", blog.getContentNoTag())
                    .endObject();
            IndexResponse indexResponse = transportClient.prepareIndex("sclience", "blog", blog.getId().toString()).setSource(contentBuilder).get();
            return indexResponse.status().toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String updateIndex(Blog blog) {
        try {
            TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("212.64.29.78"), 9300));
            XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
            contentBuilder.startObject()
                    .field("id", blog.getId())
                    .field("title", blog.getTitle())
                    .field("releaseDate", blog.getReleaseDateStr())
                    .field("content", blog.getContentNoTag())
                    .endObject();
            UpdateResponse updateResponse = transportClient.prepareUpdate("sclience", "blog", blog.getId().toString()).setDoc(contentBuilder).get();
            return updateResponse.status().toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String deleteIndex(String blogId) {
        try {
            TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("212.64.29.78"), 9300));
            DeleteResponse deleteResponse = transportClient.prepareDelete("sclience", "blog", blogId).get();
            return deleteResponse.status().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }

    }

    public List<Blog> searchBlog(String keyword) {
        List<Blog> blogs = new ArrayList<Blog>();
        try {
            TransportClient transportClient = new PreBuiltTransportClient(Settings.EMPTY).addTransportAddress(new TransportAddress(InetAddress.getByName("212.64.29.78"), 9300));
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyword, "title","content");
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightBuilder.requireFieldMatch(false).field("title").field("content").preTags("<strong><b><font color='red'>").postTags("</font></b></strong>");
            SearchResponse searchResponse = transportClient.prepareSearch("sclience").setTypes("blog").highlighter(highlightBuilder).highlighter(highlightBuilder).setQuery(multiMatchQueryBuilder).get();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                Blog blog = new Blog();
                for (Map.Entry<String, HighlightField> entry : highlightFields.entrySet()) {
                    HighlightField highlightField = highlightFields.get(entry.getKey());
                    if (entry.getKey().equals("title")) {
                        blog.setTitle(highlightField.fragments()[0].string());
                    }
                    if (entry.getKey().equals("content")) {
                        blog.setContent(highlightField.fragments()[0].string());
                    }
                }
                if (blog.getContent()==null){
                    String content = (String) sourceAsMap.get("content");
                    blog.setContent(content);
                }
                if (blog.getTitle()==null){
                    String title = (String) sourceAsMap.get("title");
                    blog.setTitle(title);
                }
                String releaseDateStr = (String) sourceAsMap.get("releaseDate");
                Integer id = (Integer) sourceAsMap.get("id");
                blog.setId(id);
                blog.setReleaseDateStr(releaseDateStr);
                blogs.add(blog);
            }
        } catch (UnknownHostException e) {
            return blogs;
        }
        return blogs;
    }
}
