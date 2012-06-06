/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.filter;

import com.thoughtworks.xstream.XStream;
import de.dimm.vsm.Utilities.ZipUtilities;

import java.util.ArrayList;




/**
 *
 * @author mw
 */
public class FilterMatcher
{
    ArrayList<LogicEntry> list;

    FilterValProvider f_provider;

    public FilterMatcher( ArrayList<LogicEntry> list, FilterValProvider f_provider )
    {
        this.list = list;
        this.f_provider = f_provider;
    }



    public boolean eval()
    {
        GroupEntry ge = new GroupEntry();
        ge.children = list;
        
        boolean ret = ge.eval( f_provider );

        return ret;
    }

    public static ArrayList<LogicEntry> get_filter_list( String compressed_list_str, boolean compressed )
    {
        ArrayList<LogicEntry> list = null;
        if (compressed_list_str.length() == 0)
        {
            list = new ArrayList<LogicEntry>();
        }
        else
        {
            try
            {
                String xml_list_str = compressed_list_str;
                if (compressed)
                    xml_list_str = ZipUtilities.uncompress(compressed_list_str);

                XStream xstr = new XStream();

                Object o = xstr.fromXML(xml_list_str);
                if (o instanceof ArrayList<?>)
                {
                    list = (ArrayList<LogicEntry>) o;
                }
                else
                    throw new Exception("wrong list data type" );
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return list;
    }



}
