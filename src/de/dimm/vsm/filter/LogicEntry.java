/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package de.dimm.vsm.filter;

import java.util.ArrayList;

/**
 *
 * @author mw
 */
public abstract class LogicEntry
{
    private boolean neg;
    private boolean previous_is_or;
    private ArrayList<LogicEntry> parent_list;

    public LogicEntry( ArrayList<LogicEntry> parent_list, boolean neg, boolean previuos_is_or )
    {
        this.parent_list = parent_list;
        this.neg = neg;
        this.previous_is_or = previuos_is_or;
    }


    public abstract boolean eval(FilterValProvider f_provider);

    /**
     * @return the neg
     */
    public boolean isNeg()
    {
        return neg;
    }

    /**
     * @param neg the neg to set
     */
    public void setNeg( boolean neg )
    {
        this.neg = neg;
    }

    /**
     * @return the previous_is_or
     */
    public boolean isPrevious_is_or()
    {
        return previous_is_or;
    }

    /**
     * @param previous_is_or the previous_is_or to set
     */
    public void setPrevious_is_or( boolean previous_is_or )
    {
        this.previous_is_or = previous_is_or;
    }

    /**
     * @return the parent_list
     */
    public ArrayList<LogicEntry> getParent_list()
    {
        return parent_list;
    }

}
