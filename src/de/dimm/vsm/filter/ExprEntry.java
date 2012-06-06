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
public class ExprEntry extends LogicEntry
{

    public enum OPERATION
    {
        BEGINS_WITH,
        ENDS_WITH,
        CONTAINS,
        CONTAINS_SUBSTR,
        REGEXP,
        NUM_EQUAL,
        NUM_LT,
        NUM_GT,
        EXACT
    }
    public enum TYPE
    {
        STRING,
        INT,
        TIMESTAMP
    }

    private String name;
    private String value;
    private OPERATION operation;
    private TYPE type;

    /**
     * @return the name
     */
    public String getName()
    {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName( String name )
    {
        this.name = name;
    }

    /**
     * @return the value
     */
    public String getValue()
    {
        return value;
    }

    /**
     * @param value the value to set
     */
    public void setValue( String value )
    {
        this.value = value;
    }

    /**
     * @return the operation
     */
    public OPERATION getOperation()
    {
        return operation;
    }

    /**
     * @param operation the operation to set
     */
    public void setOperation( OPERATION operation )
    {
        this.operation = operation;
    }
    
    /**
     * @return the type
     */
    public TYPE getType()
    {
        return type;
    }

    /**
     * @param type the type to set
     */
    public void setType( TYPE type )
    {
        this.type = type;
    }

    public ExprEntry()
    {
        super(null, false,  false);
        this.type = TYPE.STRING;
    }


    public ExprEntry( ArrayList<LogicEntry> parent_list, String name, String value, OPERATION operation, TYPE type, boolean neg, boolean previous_is_or )
    {
        super(parent_list, neg,  previous_is_or);
        this.name = name;
        this.value = value.toLowerCase();
        this.operation = operation;
        this.type = type;
    }

    @Override
    public boolean eval(FilterValProvider f_provider)
    {
        boolean ret = false;

        ArrayList<String> provider_val_list = f_provider.get_val_vor_name(name);
        if (provider_val_list != null)
        {
            // EVAL ALL RESULT STRINGS
            for (int i = 0; i < provider_val_list.size(); i++)
            {
                String provider_val = provider_val_list.get(i);

                switch( operation )
                {
                    case BEGINS_WITH:   ret = provider_val.toLowerCase().startsWith(value); break;
                    case ENDS_WITH:     ret = provider_val.toLowerCase().endsWith(value); break;
                    case CONTAINS_SUBSTR:      ret = provider_val.toLowerCase().indexOf(value) >= 0; break;
                    case CONTAINS:       
                    {
                        if (provider_val.toLowerCase().indexOf(value) != -1)
                        {
                            ret = true;
                            break;
                        }
                        String[] s = provider_val.toLowerCase().split("[\\. ;,@]");
                        if (s.length > 0)
                        {
                            for (int j = 0; j < s.length; j++)
                            {
                                String string = s[j];
                                if (string.compareTo(value) == 0)
                                {
                                    ret = true; 
                                    break;
                                }                                
                            }
                        }
                        else
                        {
                            ret = provider_val.toLowerCase().compareTo(value) == 0;
                        }
                        break;
                    }
                    case REGEXP:        ret = provider_val.matches(value); break;
                    case EXACT:         ret = provider_val.equals(value); break;
                    case NUM_EQUAL:
                    case NUM_GT:
                    case NUM_LT:
                    {
                        try
                        {
                            long l1 = Long.parseLong(provider_val);
                            long l2 = Long.parseLong(value);
                            if (operation == OPERATION.NUM_EQUAL)
                            {
                                ret = l1 == l2;
                            }
                            if (operation == OPERATION.NUM_GT)
                            {
                                ret = l1 > l2;
                            }
                            if (operation == OPERATION.NUM_LT)
                            {
                                ret = l1 > l2;
                            }
                        }
                        catch (NumberFormatException exc)
                        {
                            ret = false;
                        }
                        break;
                    }
                }

                if (ret)
                    break;
            }
        }

        if (isNeg())
            ret = !ret;

        return ret;
    }
}
