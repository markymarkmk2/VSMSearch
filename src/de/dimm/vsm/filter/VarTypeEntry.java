/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.filter;

import de.dimm.vsm.filter.ExprEntry.TYPE;

/**
 *
 * @author mw
 */
public class VarTypeEntry
{
    String var;
    ExprEntry.TYPE type;

    public VarTypeEntry( String var, TYPE type )
    {
        this.var = var;
        this.type = type;
    }

    public String getVar()
    {
        return var;
    }

    public TYPE getType()
    {
        return type;
    }


}
