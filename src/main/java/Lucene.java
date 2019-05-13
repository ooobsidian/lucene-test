/**
 * @program: lucene_test
 * @description: 使用lucene8.0.0中文数据库全文检索测试
 * @author: obsidian
 * @create: 2019-05-13 20:51
 */

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Date;


public class Lucene {
    private static final String driverClassName = "org.postgresql.Driver";
    private static final String url = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "root";
    private static final Version version = Version.LUCENE_47;
    private static final String PATH = "/Users/obsidian/source/lucene_test/src/index";
    private Directory directory = null;
    private DirectoryReader directoryReader = null;
    private IndexWriter indexWriter = null;
    private IKAnalyzer ikAnalyzer;
    private Connection connection;

    public Lucene() throws IOException {
        directory = FSDirectory.open(new File(PATH));
    }

    //    分析器
    public IndexSearcher getSearcher() {
        try {
            if (directoryReader == null) {
                directoryReader = DirectoryReader.open(directory);
            } else {
                DirectoryReader tr = DirectoryReader.openIfChanged(directoryReader);
                if (tr != null) {
                    directoryReader.close();
                    directoryReader = tr;
                }
            }
            return new IndexSearcher(directoryReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //    连接数据库
    public Connection getConnection() {
        if (this.connection == null) {
            try {
                Class.forName(driverClassName);
                connection = DriverManager.getConnection(url, username, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    //    创建索引
    public void createIndex() {
        Connection connection = getConnection();
        ResultSet resultSet = null;
        PreparedStatement pst = null;
        if (connection == null) {
            System.out.println("数据库连接失败");
        }
        String sql = "select * from tbl_news";
        try {
            pst = connection.prepareStatement(sql);
            resultSet = pst.executeQuery();
            IndexWriterConfig iwc = new IndexWriterConfig(version, getAnalyzer());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            indexWriter = new IndexWriter(directory, iwc);
            while (resultSet.next()) {
                String title = resultSet.getString(7);
                String newsurl = resultSet.getString(8);
                String content = resultSet.getString(15);
                Date create_time = resultSet.getDate(17);
                Document doc = new Document();
                doc.add(new TextField("news_title", title + "", Field.Store.YES));
                doc.add(new TextField("news_url", newsurl + "", Field.Store.YES));
                doc.add(new TextField("news_content", content + "", Field.Store.YES));
                doc.add(new TextField("news_create_time", create_time + "", Field.Store.YES));
                indexWriter.addDocument(doc);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (indexWriter != null)
                    indexWriter.close();
                resultSet.close();
                pst.close();
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //    检索
    public void searchByTerm(String field, String keyword, int num) {
        long startTime = System.currentTimeMillis();    //获取开始时间
        IndexSearcher indexSearcher = getSearcher();
        IKAnalyzer analyzer = getAnalyzer();
        //使用QueryParser查询分析器构造Query对象
        QueryParser queryParser = new QueryParser(version, field, analyzer);
        queryParser.setDefaultOperator(QueryParser.AND_OPERATOR);
        try {
            Query query = queryParser.parse(keyword);
            ScoreDoc[] hits;
            hits = indexSearcher.search(query, num).scoreDocs;
            long endTime = System.currentTimeMillis();
            System.out.println("搜索到以下内容（共" + num + "条，耗时"+(endTime - startTime)+"ms）：");
            for (int i = 0; i < hits.length; i++) {
                Document doc = indexSearcher.doc(hits[i].doc);
                System.out.println(doc.get("news_title") + " " + doc.get("news_content") + " " + doc.get("news_url") + " " + doc.get("news_create_time"));
            }
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private IKAnalyzer getAnalyzer() {
        if (ikAnalyzer == null) {
            return new IKAnalyzer();
        } else {
            return ikAnalyzer;
        }
    }

    //主函数
    public static void main(String[] args) throws IOException {
        Lucene ld = new Lucene();
        ld.createIndex();
    }
}

class Test {
    public static void main(String... args) throws IOException {

        Lucene ld = new Lucene();
        ld.searchByTerm("news_content", "上海大学", 10);

    }
}