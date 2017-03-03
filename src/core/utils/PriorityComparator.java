package core.utils;

import java.util.Comparator;


/*
 * Priority comparator for Rein
 */

public class PriorityComparator implements Comparator<Operation> {
    @Override
    public int compare(Operation first, Operation second) {
         if(first instanceof PriorityProvider && second instanceof PriorityProvider)
                 return ((PriorityProvider)first).getPriority().compareTo(((PriorityProvider)second).getPriority());
         else
             throw new UnsupportedOperationException();
    }

}