package de.dimm.vsm.search;

import java.io.Reader;
import java.io.Serializable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharTokenizer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;

public class FileNameAnalyzer extends Analyzer implements Serializable
{

    public class DelimiterAnalyzer extends Analyzer
    {

        private final char[] fDelim;

        public DelimiterAnalyzer( char[] delim )
        {
            fDelim = delim;
        }

        @Override
        public TokenStream tokenStream( String fieldName, Reader reader )
        {

            TokenStream result = new CharTokenizer(reader)
            {

                @Override
                protected boolean isTokenChar( char c )
                {
                    for (int i = 0; i < fDelim.length; i++)
                    {
                        char d = fDelim[i];
                        if (d == c )
                            return false;
                    }
                    return true;
                }
            };            
            return result;
        }
    }

    @Override
    public final TokenStream tokenStream( String fieldName, final Reader reader )
    {
        char[] fDelim = {' ', '\t'};
        TokenStream result = new DelimiterAnalyzer(fDelim).tokenStream(fieldName, reader);
        result = new FileNameFilter(result);
        result = new LowerCaseFilter(result);
        return result;
    }
}
