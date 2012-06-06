/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.search;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;

/**
 *
 * @author Administrator
 */
public interface IndexApi
{


    public boolean init();
    public boolean isOpen();
    public boolean create() throws IOException;
    public boolean open() throws IOException;
    public boolean close() throws IOException;

    public Analyzer create_analyzer( String language, boolean do_index );

    Document addIndex(  List<Field> fields );

    boolean writeDocument( Document d ) throws IOException;
    void updateDocument( Document doc, Query qry ) throws CorruptIndexException, IOException;
    void removeDocument( Query qry) throws CorruptIndexException, IOException;
    //void removeDocument( Term[] term ) throws CorruptIndexException, IOException;

    List<Document> searchDocument( Query qry, Filter filter, int n, Sort sort );

    public void flush( boolean optimize) throws IOException;

   
    
}
