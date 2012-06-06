/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.search;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Stack;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 *
 * @author Administrator
 */
public class FileNameFilter extends TokenFilter implements Serializable
{

    private Stack<String> fileTokenStack;

    TermAttribute termAtt;
    PositionIncrementAttribute posIncrAtt;

    public FileNameFilter( TokenStream in )
    {
        super(in);

        fileTokenStack = new Stack<String>();
        termAtt = addAttribute(TermAttribute.class);
        posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException
    {
        if (fileTokenStack.size() > 0)
        {
            termAtt.setTermBuffer(fileTokenStack.pop());
            return true;
        }
        if (!input.incrementToken())
        {
            return false;
        }
        scanToken();

        if (fileTokenStack.size() > 0)
        {
            termAtt.setTermBuffer(fileTokenStack.pop());
        }

        return true;
    }

    private void scanToken() throws IOException
    {
        String fname = String.copyValueOf(termAtt.termBuffer(), 0, termAtt.termLength() );

        fileTokenStack.push(fname.trim());
        
        fname = fname.replace(',', ' ');
        fname = fname.replace(';', ' ');
        fname = fname.replace('.', ' ');
        fname = fname.replace('-', ' ');
        fname = fname.replace('_', ' ');

        String[] parts = extractFilenameParts(fname);

        
        for (int i = 0; i < parts.length; i++)
        {
            String s = parts[i].trim();
            if (s.length() > 0)
            {
                fileTokenStack.push( parts[i].trim() );
            }
        }
    }

    private String[] extractWhitespaceParts( String fname )
    {
        String[] whitespaceParts = fname.split(" ");
        return whitespaceParts;
    }

    private String[] extractFilenameParts( String fname )
    {
        ArrayList<String> partsList = new ArrayList<String>();

        String[] whitespaceParts = extractWhitespaceParts(fname);

        for (int w = 0; w < whitespaceParts.length; w++)
        {
            String p = whitespaceParts[w];
            if (p.isEmpty())
                continue;
            
            char lastCh = p.charAt(0);
            int start = 0;
            
            for ( int i = 1; i < p.length(); i++)
            {
                char ch =  p.charAt(i);

                // DETECT CAMELCASE, MIN LEN == 3
                int dl = i - start;

                if ( dl > 2 && !Character.isDigit(ch) && !Character.isDigit(lastCh) && Character.isUpperCase(ch) && !Character.isUpperCase( lastCh ))
                {
                    if (i + 3 <= p.length() && Character.isLowerCase(p.charAt(i + 1)) && Character.isLowerCase(p.charAt(i + 2)) );
                    {
                        partsList.add( p.substring(start, i) );
                        start = i;
                    }
                }
                // DETECT DIGIT BORDER
                if (!Character.isDigit(ch) && Character.isDigit(lastCh))
                {
                    partsList.add( p.substring(start, i) );
                    start = i;
                }
                // DETECT DIGIT BORDER
                if (Character.isDigit(ch) && !Character.isDigit(lastCh))
                {
                    partsList.add( p.substring(start, i) );
                    start = i;
                }

                lastCh = ch;
            }

            if (start < p.length())
            {
                partsList.add( p.substring(start) );
            }
        }
        if (partsList.isEmpty())
            return new String[] { fname };

        return partsList.toArray(new String[0]);
    }

}
