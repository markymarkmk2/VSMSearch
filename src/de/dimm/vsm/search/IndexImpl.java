/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.search;

import de.dimm.vsm.log.LogManager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 *
 * @author Administrator
 */
public class IndexImpl implements IndexApi
{
    private long next_flush_optimize_ts;
    private static long FLUSH_OPTIMIZE_CYCLE_MS = 60 * 60 * 1000;
    private long minFreeSpace = 100000000l;  // 100 MB
    private long lowFreeSpace = 10000000000l; // 10GB
    boolean low_space_left;
    boolean no_space_left;
    public static int LUC_OPTIMIZE_FILES = 10; // OPTIMIZE DOWN TO 10 FILES

    private IndexReader reader;
    private IndexWriter writer;
    File indexPath;
    final String idx_lock = "";
    Map<String, String> analyzerMap;

    long readIsDirtyTS = 0;
    private long maxReaderDirtySettleTime = 10*60*1000l;  // READ INDEX IS MAX 10 MINUTES BEHIND WRITE INDEX



    public IndexImpl( File path )
    {
        indexPath = path;
    }

    public void setMinFreeSpace( long minFreeSpace )
    {
        this.minFreeSpace = minFreeSpace;
    }

    public void setLowFreeSpace( long lowFreeSpace )
    {
        this.lowFreeSpace = lowFreeSpace;
    }
    

    @Override
    public boolean init()
    {

        analyzerMap = new LinkedHashMap<String, String>();
        analyzerMap.put("en", "org.apache.lucene.analysis.StandardAnanlyzer");
        analyzerMap.put("pt", "org.apache.lucene.analysis.br.BrazilianAnalyzer");
        analyzerMap.put("zh", "org.apache.lucene.analysis.cn.ChineseAnalyzer");
        analyzerMap.put("cs", "org.apache.lucene.analysis.cz.CzechAnalyzer");
        analyzerMap.put("de", "org.apache.lucene.analysis.de.GermanAnalyzer");
        analyzerMap.put("el", "org.apache.lucene.analysis.el.GreekAnalyzer");
        analyzerMap.put("fr", "org.apache.lucene.analysis.fr.FrenchAnalyzer");
        analyzerMap.put("nl", "org.apache.lucene.analysis.nl.DutchAnalyzer");
        analyzerMap.put("ru", "org.apache.lucene.analysis.ru.RussianAnalyzer");
        analyzerMap.put("ja", "org.apache.lucene.analysis.cjk.CJKAnalyzer");
        analyzerMap.put("ko", "org.apache.lucene.analysis.cjk.CJKAnalyzer");
        analyzerMap.put("th", "org.apache.lucene.analysis.th.ThaiAnalyzer");
        analyzerMap.put("tr", "org.apache.lucene.analysis.tr.TurkishAnalyzer");

        return true;
    }

    @Override
    public Analyzer create_analyzer( String language, boolean do_index )
    {
        Analyzer analyzer = null;
        if (language == null)
        {
            language = "de";
        }

        String className = null;

        try
        {
            if (analyzerMap.containsKey(language))
            {
                className = (String) analyzerMap.get(language.toLowerCase());
            }
            if (className == null)
            {
                className = (String) analyzerMap.get("de");
            }

            Class analyzerClass = Class.forName(className);
            analyzer = (Analyzer) analyzerClass.newInstance();
        }
        catch (Exception e)
        {
            analyzer = new StandardAnalyzer(Version.LUCENE_30);
        }

        PerFieldAnalyzerWrapper wrapper = new PerFieldAnalyzerWrapper(analyzer);


        if (do_index)
        {
            wrapper.addAnalyzer("name", new FileNameAnalyzer());
        }
        else
        {
            wrapper.addAnalyzer("name", new WhitespaceAnalyzer());
        }
        return wrapper;
    }

    @Override
    public boolean create() throws IOException
    {
        return open(true);
    }
    @Override
    public boolean open() throws IOException
    {
        return open( false );
    }

    
    public boolean open(boolean can_create) throws IOException
    {
        FSDirectory dir = FSDirectory.open(indexPath);
        Analyzer analyzer = create_analyzer("de", true);


        // CHECK IF INDEX EXISTS
        boolean create = false;
        if (can_create)
        {
            create = !IndexReader.indexExists(dir);
            if (create)
            {
                LogManager.msg(LogManager.LVL_WARN, LogManager.TYP_INDEX, "Creating_new_index_in" + " " + indexPath);
            }
        }

        if (IndexWriter.isLocked(dir))
        {
            LogManager.msg(LogManager.LVL_WARN, LogManager.TYP_INDEX, "Unlocking_already_locked_IndexWriter");
            IndexWriter.unlock(dir);
        }

        writer = new IndexWriter(dir, analyzer, create, new IndexWriter.MaxFieldLength(100000));
        writer.setRAMBufferSizeMB(32);
        writer.setMergeFactor(16);

        reader = IndexReader.open(dir, /*rd_only*/ true);

        LogManager.msg(LogManager.LVL_DEBUG, LogManager.TYP_INDEX, "Opening IndexWriter " + writer.toString());

        return true;
    }

    @Override
    public boolean close() throws IOException
    {
        if (!isOpen())
           return false;

        flush(true);

        reader.close();
        reader = null;

        try
        {
            if (writer != null)
            {
                LogManager.msg(LogManager.LVL_DEBUG, LogManager.TYP_INDEX, "Closing IndexWriter " + writer.toString());
                writer.close(true);
            }
        }
        catch (Exception iOException)
        {
            writer.close(true);
        }
        writer = null;
        return true;
    }

    @Override
    public Document addIndex(  List<Field> fields )
    {
        Document doc = new Document();

        for (int i = 0; i < fields.size(); i++)
        {
            Field field = fields.get(i);
            doc.add(field);
        }

        return doc;
    }

    @Override
    public boolean writeDocument( Document d ) throws CorruptIndexException, IOException
    {
        if (writer == null)
            throw new IOException("Writer is closed");

        writer.addDocument(d);
        return true;
    }


    @Override
    public void updateDocument( Document doc,Query qry ) throws CorruptIndexException, IOException
    {
        if (writer == null)
            throw new IOException("Writer is closed");

        writer.deleteDocuments(qry);
       /* writer.expungeDeletes();
        writer.commit();*/
        writer.addDocument(doc);
        
    }

    @Override
    public void removeDocument( Query qry ) throws CorruptIndexException, IOException
    {
        if (writer == null)
            throw new IOException("Writer is closed");

        writer.deleteDocuments(qry);
    }

    void reopenReader() throws IOException
    {
        FSDirectory dir = FSDirectory.open(indexPath);
        IndexReader newReader = IndexReader.open(dir, /*rd_only*/ true);

        synchronized( idx_lock )
        {
            if (reader != null)
            {
                reader.close();
            }
            reader = newReader;
        }
    }
    
    public void updateReadIndex()
    {
        // THIS TRIGGERS A REOPEN ON NEXT SEARCH
        readIsDirtyTS = 1;
    }

    IndexReader getReader() throws IOException
    {
        if (reader == null || readIsDirtyTS != 0 && (System.currentTimeMillis() - readIsDirtyTS) > maxReaderDirtySettleTime )
        {
            reopenReader();
            readIsDirtyTS = 0;
        }
        return reader;
    }


    @Override
    public List<Document> searchDocument( Query qry, Filter filter, int n, Sort sort )
    {
        long start_time = System.currentTimeMillis();

        List<Document> ret = new ArrayList<Document>();

        IndexSearcher searcher = null;
        try
        {
            searcher = new IndexSearcher( getReader() );


            Searchable[] search_arr = new Searchable[1];
            search_arr[0] = searcher;

            MultiSearcher pms = new FullParallelMultiSearcher(search_arr);
            TopDocs tdocs = pms.search(qry, filter, n, sort);


            long diff = System.currentTimeMillis() - start_time;
            ScoreDoc[] sdocs = tdocs.scoreDocs;

            //LogManager.msg_index(LogManager.LVL_DEBUG, "Suche dauerte " + diff + "ms, " + sdocs.length + " Ergebnisse gefunden");

            for (int k = sdocs.length - 1 ; k >= 0; k--)
            {
                ScoreDoc scoreDoc = sdocs[k];

                int doc_idx = scoreDoc.doc;

                Document doc = searcher.doc(doc_idx);

                ret.add(doc);
            }
            pms.close();
        }
        catch (IOException iOException)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Abbruch in Suche" + ": " + iOException.getMessage());
        }
        finally
        {
            if (searcher != null)
            {
                try
                {
                    searcher.close();
                }
                catch (IOException iOException)
                {
                }
            }
        }
        return ret;
    }

    private void set_no_space_left( boolean b )
    {
        // DETECT STATUS CHANGE
        if (no_space_left != b)
        {
            no_space_left = b;

            if (no_space_left)
            {
                LogManager.msg_index(LogManager.LVL_ERR, "Kein Platz im Indexspeicher" + ": " + indexPath.getAbsolutePath());
            }
            else
            {
                LogManager.msg_index(LogManager.LVL_INFO, "Wieder genug Platz im Indexspeicher" + ": " + indexPath.getAbsolutePath());
            }
        }
    }

    private void set_low_space_left( boolean b, double free )
    {
        if (low_space_left != b)
        {
            low_space_left = b;

            if (low_space_left)
            {
                LogManager.msg_index(LogManager.LVL_WARN, "Wenig Platz im Indexspeicher" + ": " + indexPath.getAbsolutePath());
            }
        }
    }

    // CALLED DURING FLUSH
    void check_free_space()
    {
        File tmp_path = indexPath;
        long l = tmp_path.getFreeSpace();
        //l = Long.MAX_VALUE;
        double free = l;

        set_no_space_left(free < minFreeSpace);
        set_low_space_left(free < lowFreeSpace, free);
    }

    public boolean no_space_left()
    {
        return no_space_left;
    }
    public boolean low_space_left()
    {
        return low_space_left;
    }

    @Override
    public boolean isOpen()
    {
        return writer != null;
    }

    @Override
    public void flush( boolean and_optimize ) throws IOException
    {
        if (writer == null)
            throw new IOException("Writer is closed");

        if (!isOpen())
        {
            check_free_space();
            return;
        }

        long now = System.currentTimeMillis();
        boolean optimize = false;
        if (System.currentTimeMillis() > next_flush_optimize_ts || and_optimize)
        {
            next_flush_optimize_ts = now + FLUSH_OPTIMIZE_CYCLE_MS;
            optimize = true;
        }
        try
        {
            synchronized (idx_lock)
            {
                writer.commit();
                readIsDirtyTS = now;

                if (optimize)
                {
                    LogManager.msg_index(LogManager.LVL_INFO, "Optimiere Index" + " " + indexPath.getPath());

                    writer.optimize(LUC_OPTIMIZE_FILES);

                    LogManager.msg_index(LogManager.LVL_INFO, "Optimierung beendet" + " " + indexPath.getPath());
                }
            }
        }
        catch (CorruptIndexException ex)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Index on Diskspace " + indexPath.getPath() + " is corrupted: ", ex);
            throw new IOException(ex.getMessage());
        }
        catch (IOException ex)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Index on Diskspace " + indexPath.getPath() + " cannot be accessed: ", ex);
            throw new IOException(ex.getMessage());
        }
        catch (Exception ex)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Error in Index " + indexPath.getPath() + " cannot be accessed: ", ex);
            throw new IOException(ex.getMessage());
        }


        // CHECK FOR HARD DISK LIMITS
        check_free_space();

    }




    public static String to_field( int i )
    {
        return Integer.toString(i);
    }


    public static String to_hex_field( long l )
    {
        return Long.toString(l, 16);
    }


    public static boolean doc_field_exists( Document doc, String fld )
    {
        String val = doc.get(fld);
        if (val != null)
        {
            return true;
        }

        return false;
    }

    private static int _doc_get_int( Document doc, String fld ) throws Exception
    {
        String val = doc.get(fld);
        if (val == null)
        {
            throw new Exception("field " + fld + " does not exist");
        }

        return Integer.parseInt(val);
    }

    static long _doc_get_long( Document doc, String fld, int radix ) throws Exception
    {
        String val = doc.get(fld);
        if (val == null)
        {
            throw new Exception("field " + fld + " does not exist");
        }

        return Long.parseLong(val, radix);
    }


    public static int doc_get_int( Document doc, String fld )
    {
        int ret = -1;

        try
        {
            ret = _doc_get_int(doc, fld);
        }
        catch (Exception exception)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Cannot parse int field " + fld + " from index", exception);
        }

        return ret;
    }


    public static boolean doc_get_bool( Document doc, String fld )
    {
        boolean ret = false;

        try
        {
            String val = doc.get(fld);
            if (val.charAt(0) == '1')
            {
                ret = true;
            }
        }
        catch (Exception exception)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Cannot parse bool field " + fld + " from index", exception);
        }

        return ret;
    }


    public static long doc_get_hex_long( Document doc, String fld )
    {
        long ret = -1;

        try
        {
            ret = _doc_get_long(doc, fld, 16);
        }
        catch (Exception exception)
        {
            LogManager.msg_index(LogManager.LVL_ERR, "Cannot parse hex long field " + fld + " from index", exception);
        }

        return ret;
    }

}
