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
public class GroupEntry extends LogicEntry
{
    protected ArrayList<LogicEntry> children;

    public GroupEntry(  )
    {
        super(null, false,  true);
        children = new ArrayList<LogicEntry>();
    }
    public GroupEntry( ArrayList<LogicEntry> children )
    {
        super(null, false,  false);
        this.children = children;
    }
    public GroupEntry( ArrayList<LogicEntry> parent_list, boolean neg, boolean previuos_is_or )
    {
        super(parent_list, neg,  previuos_is_or);
        children = new ArrayList<LogicEntry>();
    }
    @Override
    public boolean eval(FilterValProvider f_provider)
    {
        boolean ret = false;


        for (int i = 0; i < children.size(); i++)
        {
            LogicEntry logicEntry = children.get(i);

            boolean local_ret = logicEntry.eval(f_provider);

            // USE FIRST EVAL AS A STARTER
            if (i == 0)
                ret = local_ret;
            else
            {
                // 2., 3., ... OPERATION
                // DETECT LAST FALSE IN AN && OP
                if (!logicEntry.isPrevious_is_or() && !ret)
                {
                    ret = false;
                    break;
                }
                // DETECT ACTUAL FALSE IN AN && OP
                if (!logicEntry.isPrevious_is_or() && !local_ret)
                {
                    ret = false;
                    break;
                }

                // SET RES TO TRUE ON VALID OR
                if (logicEntry.isPrevious_is_or() && local_ret == true)
                    ret = true;
            }
        }

        if (isNeg())
            ret = !ret;

        return ret;
    }

    /**
     * @return the children
     */
    public ArrayList<LogicEntry> getChildren()
    {
        return children;
    }

   

}