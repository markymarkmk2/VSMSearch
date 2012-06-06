/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.dimm.vsm.search;

import java.io.IOException;
import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Searchable;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.ThreadInterruptedException;

abstract class MWPriorityQueue<T>
{

    protected int size;
    private int maxSize;
    protected T[] heap;

    /** Determines the ordering of objects in this priority queue.  Subclasses
    must define this one method. */
    protected abstract boolean lessThan( T a, T b );

    /**
     * This method can be overridden by extending classes to return a sentinel
     * object which will be used by {@link #initialize(int)} to fill the queue, so
     * that the code which uses that queue can always assume it's full and only
     * change the top without attempting to insert any new object.<br>
     *
     * Those sentinel values should always compare worse than any non-sentinel
     * value (i.e., {@link #lessThan} should always favor the
     * non-sentinel values).<br>
     *
     * By default, this method returns false, which means the queue will not be
     * filled with sentinel values. Otherwise, the value returned will be used to
     * pre-populate the queue. Adds sentinel values to the queue.<br>
     *
     * If this method is extended to return a non-null value, then the following
     * usage pattern is recommended:
     *
     * <pre>
     * // extends getSentinelObject() to return a non-null value.
     * PriorityQueue<MyObject> pq = new MyQueue<MyObject>(numHits);
     * // save the 'top' element, which is guaranteed to not be null.
     * MyObject pqTop = pq.top();
     * &lt;...&gt;
     * // now in order to add a new element, which is 'better' than top (after
     * // you've verified it is better), it is as simple as:
     * pqTop.change().
     * pqTop = pq.updateTop();
     * </pre>
     *
     * <b>NOTE:</b> if this method returns a non-null value, it will be called by
     * {@link #initialize(int)} {@link #size()} times, relying on a new object to
     * be returned and will not check if it's null again. Therefore you should
     * ensure any call to this method creates a new instance and behaves
     * consistently, e.g., it cannot return null if it previously returned
     * non-null.
     *
     * @return the sentinel object to use to pre-populate the queue, or null if
     *         sentinel objects are not supported.
     */
    protected T getSentinelObject()
    {
        return null;
    }

    /** Subclass constructors must call this. */
    @SuppressWarnings("unchecked")
    protected final void initialize( int maxSize )
    {
        size = 0;
        int heapSize;
        if (0 == maxSize)
        // We allocate 1 extra to avoid if statement in top()
        {
            heapSize = 2;
        }
        else
        {
            if (maxSize == Integer.MAX_VALUE)
            {
                // Don't wrap heapSize to -1, in this case, which
                // causes a confusing NegativeArraySizeException.
                // Note that very likely this will simply then hit
                // an OOME, but at least that's more indicative to
                // caller that this values is too big.  We don't +1
                // in this case, but it's very unlikely in practice
                // one will actually insert this many objects into
                // the PQ:
                heapSize = Integer.MAX_VALUE;
            }
            else
            {
                // NOTE: we add +1 because all access to heap is
                // 1-based not 0-based.  heap[0] is unused.
                heapSize = maxSize + 1;
            }
        }
        heap = (T[]) new Object[heapSize]; // T is unbounded type, so this unchecked cast works always
        this.maxSize = maxSize;

        // If sentinel objects are supported, populate the queue with them
        T sentinel = getSentinelObject();
        if (sentinel != null)
        {
            heap[1] = sentinel;
            for (int i = 2; i < heap.length; i++)
            {
                heap[i] = getSentinelObject();
            }
            size = maxSize;
        }
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time. If one tries to add
     * more objects than maxSize from initialize an
     * {@link ArrayIndexOutOfBoundsException} is thrown.
     *
     * @return the new 'top' element in the queue.
     */
    public final T add( T element )
    {
        size++;
        heap[size] = element;
        upHeap();
        return heap[1];
    }

    public T pop_top()
    {
        T ret = heap[size];
        heap[size] = null;
        size--;
        return ret;
    }

    /**
     * Adds an Object to a PriorityQueue in log(size) time.
     * It returns the object (if any) that was
     * dropped off the heap because it was full. This can be
     * the given parameter (in case it is smaller than the
     * full heap's minimum, and couldn't be added), or another
     * object that was previously the smallest value in the
     * heap and now has been replaced by a larger one, or null
     * if the queue wasn't yet full with maxSize elements.
     */
    public T insertWithOverflow( T element )
    {
        if (size < maxSize)
        {
            add(element);
            return null;
        }
        else if (size > 0 && !lessThan(element, heap[1]))
        {
            T ret = heap[1];
            heap[1] = element;
            updateTop();
            return ret;
        }
        else
        {
            return element;
        }
    }

    /** Returns the least element of the PriorityQueue in constant time. */
    public final T top()
    {
        // We don't need to check size here: if maxSize is 0,
        // then heap is length 2 array with both entries null.
        // If size is 0 then heap[1] is already null.
        return heap[1];
    }

    /** Removes and returns the least element of the PriorityQueue in log(size)
    time. */
    public final T pop()
    {
        if (size > 0)
        {
            T result = heap[1];			  // save first value
            heap[1] = heap[size];			  // move last to first
            heap[size] = null;			  // permit GC of objects
            size--;
            downHeap();				  // adjust heap
            return result;
        }
        else
        {
            return null;
        }
    }

    /**
     * Should be called when the Object at top changes values. Still log(n) worst
     * case, but it's at least twice as fast to
     *
     * <pre>
     * pq.top().change();
     * pq.updateTop();
     * </pre>
     *
     * instead of
     *
     * <pre>
     * o = pq.pop();
     * o.change();
     * pq.push(o);
     * </pre>
     *
     * @return the new 'top' element.
     */
    public final T updateTop()
    {
        downHeap();
        return heap[1];
    }

    /** Returns the number of elements currently stored in the PriorityQueue. */
    public final int size()
    {
        return size;
    }

    /** Removes all entries from the PriorityQueue. */
    public final void clear()
    {
        for (int i = 0; i <= size; i++)
        {
            heap[i] = null;
        }
        size = 0;
    }

    private final void upHeap()
    {
        int i = size;
        T node = heap[i];			  // save bottom node
        int j = i >>> 1;
        while (j > 0 && lessThan(node, heap[j]))
        {
            heap[i] = heap[j];			  // shift parents down
            i = j;
            j = j >>> 1;
        }
        heap[i] = node;				  // install saved node
    }

    private final void downHeap()
    {
        int i = 1;
        T node = heap[i];			  // save top node
        int j = i << 1;				  // find smaller child
        int k = j + 1;
        if (k <= size && lessThan(heap[k], heap[j]))
        {
            j = k;
        }
        while (j <= size && lessThan(heap[j], node))
        {
            heap[i] = heap[j];			  // shift up child
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && lessThan(heap[k], heap[j]))
            {
                j = k;
            }
        }
        heap[i] = node;				  // install saved node
    }
}

class MWHitQueue extends MWPriorityQueue<ScoreDoc>
{

    private boolean prePopulate;

    /**
     * Creates a new instance with <code>size</code> elements. If
     * <code>prePopulate</code> is set to true, the queue will pre-populate itself
     * with sentinel objects and set its {@link #size()} to <code>size</code>. In
     * that case, you should not rely on {@link #size()} to get the number of
     * actual elements that were added to the queue, but keep track yourself.<br>
     * <b>NOTE:</b> in case <code>prePopulate</code> is true, you should pop
     * elements from the queue using the following code example:
     *
     * <pre>
     * PriorityQueue pq = new HitQueue(10, true); // pre-populate.
     * ScoreDoc top = pq.top();
     *
     * // Add/Update one element.
     * top.score = 1.0f;
     * top.doc = 0;
     * top = (ScoreDoc) pq.updateTop();
     * int totalHits = 1;
     *
     * // Now pop only the elements that were *truly* inserted.
     * // First, pop all the sentinel elements (there are pq.size() - totalHits).
     * for (int i = pq.size() - totalHits; i &gt; 0; i--) pq.pop();
     *
     * // Now pop the truly added elements.
     * ScoreDoc[] results = new ScoreDoc[totalHits];
     * for (int i = totalHits - 1; i &gt;= 0; i--) {
     *   results[i] = (ScoreDoc) pq.pop();
     * }
     * </pre>
     *
     * <p><b>NOTE</b>: This class pre-allocate a full array of
     * length <code>size</code>.
     *
     * @param size
     *          the requested size of this queue.
     * @param prePopulate
     *          specifies whether to pre-populate the queue with sentinel values.
     * @see #getSentinelObject()
     */
    MWHitQueue( int size, boolean prePopulate )
    {
        this.prePopulate = prePopulate;
        initialize(size);
    }

    // Returns null if prePopulate is false.
    @Override
    protected ScoreDoc getSentinelObject()
    {
        // Always set the doc Id to MAX_VALUE so that it won't be favored by
        // lessThan. This generally should not happen since if score is not NEG_INF,
        // TopScoreDocCollector will always add the object to the queue.
        return !prePopulate ? null : new ScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY);
    }

    @Override
    protected final boolean lessThan( ScoreDoc hitA, ScoreDoc hitB )
    {
        if (hitA.score == hitB.score)
        {
            return hitA.doc > hitB.doc;
        }
        else
        {
            return hitA.score < hitB.score;
        }
    }
}

class MWMultiSearcherCallableWithSort implements Callable<TopFieldDocs>
{

    private final Lock lock;
    private final Searchable searchable;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final int i;
    private final MWFieldDocSortedHitQueue hq;
    private final int[] starts;
    private final Sort sort;

    public MWMultiSearcherCallableWithSort( Lock lock, Searchable searchable, Weight weight,
            Filter filter, int nDocs, MWFieldDocSortedHitQueue hq, Sort sort, int i, int[] starts )
    {
        this.lock = lock;
        this.searchable = searchable;
        this.weight = weight;
        this.filter = filter;
        this.nDocs = nDocs;
        this.hq = hq;
        this.i = i;
        this.starts = starts;
        this.sort = sort;
    }

    @Override
    public TopFieldDocs call() throws IOException
    {
        final TopFieldDocs docs = searchable.search(weight, filter, nDocs, sort);
        // If one of the Sort fields is FIELD_DOC, need to fix its values, so that
        // it will break ties by doc Id properly. Otherwise, it will compare to
        // 'relative' doc Ids, that belong to two different searchables.
        for (int j = 0; j < docs.fields.length; j++)
        {
            if (docs.fields[j].getType() == SortField.DOC)
            {
                // iterate over the score docs and change their fields value
                for (int j2 = 0; j2 < docs.scoreDocs.length; j2++)
                {
                    FieldDoc fd = (FieldDoc) docs.scoreDocs[j2];
                    fd.fields[j] = Integer.valueOf(((Integer) fd.fields[j]).intValue() + starts[i]);
                }
                break;
            }
        }

        lock.lock();
        try
        {
            hq.setFields(docs.fields);
        }
        finally
        {
            lock.unlock();
        }

        final ScoreDoc[] scoreDocs = docs.scoreDocs;
        for (int j = 0; j < scoreDocs.length; j++)
        { // merge scoreDocs into hq
            final FieldDoc fieldDoc = (FieldDoc) scoreDocs[j];
            fieldDoc.doc += starts[i]; // convert doc
            //it would be so nice if we had a thread-safe insert
            lock.lock();
            try
            {
                if (fieldDoc == hq.insertWithOverflow(fieldDoc))
                {
                    break;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return docs;
    }
}

/**
 * A thread subclass for searching a single searchable
 */
class MWMultiSearcherCallableNoSort implements Callable<TopDocs>
{

    private final Lock lock;
    private final Searchable searchable;
    private final Weight weight;
    private final Filter filter;
    private final int nDocs;
    private final int i;
    private final MWHitQueue hq;
    private final int[] starts;

    public MWMultiSearcherCallableNoSort( Lock lock, Searchable searchable, Weight weight,
            Filter filter, int nDocs, MWHitQueue hq, int i, int[] starts )
    {
        this.lock = lock;
        this.searchable = searchable;
        this.weight = weight;
        this.filter = filter;
        this.nDocs = nDocs;
        this.hq = hq;
        this.i = i;
        this.starts = starts;
    }

    @Override
    public TopDocs call() throws IOException
    {
        final TopDocs docs = searchable.search(weight, filter, nDocs);
        final ScoreDoc[] scoreDocs = docs.scoreDocs;
        for (int j = 0; j < scoreDocs.length; j++)
        { // merge scoreDocs into hq
            final ScoreDoc scoreDoc = scoreDocs[j];
            scoreDoc.doc += starts[i]; // convert doc
            //it would be so nice if we had a thread-safe insert
            lock.lock();
            try
            {
                if (scoreDoc == hq.insertWithOverflow(scoreDoc))
                {
                    break;
                }
            }
            finally
            {
                lock.unlock();
            }
        }
        return docs;
    }
}


class MWFieldDocSortedHitQueue extends MWPriorityQueue<FieldDoc>
{

    volatile SortField[] fields = null;
    // used in the case where the fields are sorted by locale
    // based strings
    volatile Collator[] collators = null;

    /**
     * Creates a hit queue sorted by the given list of fields.
     * @param fields Fieldable names, in priority order (highest priority first).
     * @param size  The number of hits to retain.  Must be greater than zero.
     */
    MWFieldDocSortedHitQueue( int size )
    {
        initialize(size);
    }

    /**
     * Allows redefinition of sort fields if they are <code>null</code>.
     * This is to handle the case using ParallelMultiSearcher where the
     * original list contains AUTO and we don't know the actual sort
     * type until the values come back.  The fields can only be set once.
     * This method should be synchronized external like all other PQ methods.
     * @param fields
     */
    void setFields( SortField[] fields )
    {
        this.fields = fields;
        this.collators = hasCollators(fields);
    }

    /** Returns the fields being used to sort. */
    SortField[] getFields()
    {
        return fields;
    }

    /** Returns an array of collators, possibly <code>null</code>.  The collators
     * correspond to any SortFields which were given a specific locale.
     * @param fields Array of sort fields.
     * @return Array, possibly <code>null</code>.
     */
    private Collator[] hasCollators( final SortField[] fields )
    {
        if (fields == null)
        {
            return null;
        }
        Collator[] ret = new Collator[fields.length];
        for (int i = 0; i < fields.length; ++i)
        {
            Locale locale = fields[i].getLocale();
            if (locale != null)
            {
                ret[i] = Collator.getInstance(locale);
            }
        }
        return ret;
    }

    /**
     * Returns whether <code>a</code> is less relevant than <code>b</code>.
     * @param a ScoreDoc
     * @param b ScoreDoc
     * @return <code>true</code> if document <code>a</code> should be sorted after document <code>b</code>.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected final boolean lessThan( final FieldDoc docA, final FieldDoc docB )
    {
        final int n = fields.length;
        int c = 0;
        for (int i = 0; i < n && c == 0; ++i)
        {
            final int type = fields[i].getType();
            if (type == SortField.STRING)
            {
                final String s1 = (String) docA.fields[i];
                final String s2 = (String) docB.fields[i];
                // null values need to be sorted first, because of how FieldCache.getStringIndex()
                // works - in that routine, any documents without a value in the given field are
                // put first.  If both are null, the next SortField is used
                if (s1 == null)
                {
                    c = (s2 == null) ? 0 : -1;
                }
                else if (s2 == null)
                {
                    c = 1;
                }
                else if (fields[i].getLocale() == null)
                {
                    c = s1.compareTo(s2);
                }
                else
                {
                    c = collators[i].compare(s1, s2);
                }
            }
            else
            {
                c = docA.fields[i].compareTo(docB.fields[i]);
                if (type == SortField.SCORE)
                {
                    c = -c;
                }
            }
            // reverse sort
            if (fields[i].getReverse())
            {
                c = -c;
            }
        }

        // avoid random sort order that could lead to duplicates (bug #31241):
        if (c == 0)
        {
            return docA.doc > docB.doc;
        }

        return c > 0;
    }

    @Override
    public FieldDoc insertWithOverflow( FieldDoc element )
    {
        return super.insertWithOverflow(element);
    }



    void sort()
    {
        Comparator<FieldDoc> comp = new Comparator<FieldDoc>()
        {

            @Override
            public int compare( FieldDoc docA, FieldDoc docB )
            {
                final int n = fields.length;
                int c = 0;
                for (int i = 0; i < n && c == 0; ++i)
                {
                    final int type = fields[i].getType();
                    if (type == SortField.STRING)
                    {
                        final String s1 = (String) docA.fields[i];
                        final String s2 = (String) docB.fields[i];
                        // null values need to be sorted first, because of how FieldCache.getStringIndex()
                        // works - in that routine, any documents without a value in the given field are
                        // put first.  If both are null, the next SortField is used
                        if (s1 == null)
                        {
                            c = (s2 == null) ? 0 : -1;
                        }
                        else if (s2 == null)
                        {
                            c = 1;
                        }
                        else if (fields[i].getLocale() == null)
                        {
                            c = s1.compareTo(s2);
                        }
                        else
                        {
                            c = collators[i].compare(s1, s2);
                        }
                    }
                    else
                    {
                        if (docA == null)
                        {
                            if (docB == null)
                                return 0;

                            return 1;
                        }
                        if (docB == null)
                            return -1;
                        
                        c = docA.fields[i].compareTo(docB.fields[i]);
                        if (type == SortField.SCORE)
                        {
                            c = -c;
                        }
                    }
                    // reverse sort
                    if (fields[i].getReverse())
                    {
                        c = -c;
                    }
                }

                // avoid random sort order that could lead to duplicates (bug #31241):
                if (c == 0)
                {
                    return docB.doc - docA.doc;
                }

                // QUEUE IS SORTED UPSIDE DOWN
                return -c;
            }
        };
        Arrays.sort(heap, 1, size() + 1, comp);

    }

}
/**
 *
 * @author mw
 */
public class FullParallelMultiSearcher extends MultiSearcher
{

    private final ExecutorService executor;
    private final Searchable[] searchables;
    private final int[] starts;

    /** Creates a {@link Searchable} which searches <i>searchables</i>. */
    public FullParallelMultiSearcher( Searchable... searchables ) throws IOException
    {
        super(searchables);
        this.searchables = searchables;
        this.starts = getStarts();
        executor = Executors.newCachedThreadPool(new NamedThreadFactory(this.getClass().getSimpleName()));
    }

    /**
     * Executes each {@link Searchable}'s docFreq() in its own thread and waits for each search to complete and merge
     * the results back together.
     */
    @Override
    public int docFreq( final Term term ) throws IOException
    {
        final ExecutionHelper<Integer> runner = new ExecutionHelper<Integer>(executor);
        for (int i = 0; i < searchables.length; i++)
        {
            final Searchable searchable = searchables[i];
            runner.submit(new Callable<Integer>()
            {

                @Override
                public Integer call() throws IOException
                {
                    return Integer.valueOf(searchable.docFreq(term));
                }
            });
        }
        int docFreq = 0;
        for (Integer num : runner)
        {
            docFreq += num.intValue();
        }
        return docFreq;
    }

    /**
     * A search implementation which executes each
     * {@link Searchable} in its own thread and waits for each search to complete and merge
     * the results back together.
     */
    @Override
    public TopDocs search( Weight weight, Filter filter, int nDocs ) throws IOException
    {
        final MWHitQueue hq = new MWHitQueue(nDocs, false);
        final Lock lock = new ReentrantLock();
        final ExecutionHelper<TopDocs> runner = new ExecutionHelper<TopDocs>(executor);

        for (int i = 0; i < searchables.length; i++)
        { // search each searchable
            runner.submit(
                    new MWMultiSearcherCallableNoSort(lock, searchables[i], weight, filter, nDocs, hq, i, starts));
        }

        int totalHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (final TopDocs topDocs : runner)
        {
            totalHits += topDocs.totalHits;
            maxScore = Math.max(maxScore, topDocs.getMaxScore());
        }

        final ScoreDoc[] scoreDocs = new ScoreDoc[hq.size()];
        for (int i = hq.size() - 1; i >= 0; i--) // put docs in array
        {
            scoreDocs[i] = hq.pop();
        }

        return new TopDocs(totalHits, scoreDocs, maxScore);
    }

    /**
     * A search implementation allowing sorting which spans a new thread for each
     * Searchable, waits for each search to complete and merges
     * the results back together.
     */
    @Override
    public TopFieldDocs search( Weight weight, Filter filter, int nDocs, Sort sort ) throws IOException
    {
        if (sort == null)
        {
            throw new NullPointerException();
        }

//    final FieldDocSortedHitQueue hq = new FieldDocSortedHitQueue(nDocs);
        // MW:ALLOW SEARCH FOR N RESULTS IN EACH SEARCHABLE
        final MWFieldDocSortedHitQueue hq = new MWFieldDocSortedHitQueue(nDocs * searchables.length);

        final Lock lock = new ReentrantLock();
        final ExecutionHelper<TopFieldDocs> runner = new ExecutionHelper<TopFieldDocs>(executor);
        for (int i = 0; i < searchables.length; i++)
        { // search each searchable
            runner.submit(
                    new MWMultiSearcherCallableWithSort(lock, searchables[i], weight, filter, nDocs, hq, sort, i, starts));
        }
        int totalHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        for (final TopFieldDocs topFieldDocs : runner)
        {
            totalHits += topFieldDocs.totalHits;
            maxScore = Math.max(maxScore, topFieldDocs.getMaxScore());
        }
        int ret_size = hq.size();
        if (ret_size > nDocs)
        {
            ret_size = nDocs;
        }

        hq.sort();
        
        final ScoreDoc[] scoreDocs = new ScoreDoc[ret_size];
        for (int i = ret_size - 1; i >= 0; i--) // put docs in array
        {
            scoreDocs[i] = hq.pop_top();
        }

        return new TopFieldDocs(totalHits, scoreDocs, hq.getFields(), maxScore);
    }

    /** Lower-level search API.
     *
     * <p>{@link Collector#collect(int)} is called for every matching document.
     *
     * <p>Applications should only use this if they need <i>all</i> of the
     * matching documents.  The high-level search API ({@link
     * Searcher#search(Query,int)}) is usually more efficient, as it skips
     * non-high-scoring hits.
     *
     * <p>This method cannot be parallelized, because {@link Collector}
     * supports no concurrent access.
     *
     * @param weight to match documents
     * @param filter if non-null, a bitset used to eliminate some documents
     * @param collector to receive hits
     */
    @Override
    public void search( final Weight weight, final Filter filter, final Collector collector )
            throws IOException
    {
        for (int i = 0; i < searchables.length; i++)
        {

            final int start = starts[i];

            final Collector hc = new Collector()
            {

                @Override
                public void setScorer( final Scorer scorer ) throws IOException
                {
                    collector.setScorer(scorer);
                }

                @Override
                public void collect( final int doc ) throws IOException
                {
                    collector.collect(doc);
                }

                @Override
                public void setNextReader( final IndexReader reader, final int docBase ) throws IOException
                {
                    collector.setNextReader(reader, start + docBase);
                }

                @Override
                public boolean acceptsDocsOutOfOrder()
                {
                    return collector.acceptsDocsOutOfOrder();
                }
            };

            searchables[i].search(weight, filter, hc);
        }
    }

    @Override
    public void close() throws IOException
    {
        executor.shutdown();
        super.close();
    }

    /**
     * A helper class that wraps a {@link CompletionService} and provides an
     * iterable interface to the completed {@link Callable} instances.
     *
     * @param <T>
     *          the type of the {@link Callable} return value
     */
    private static final class ExecutionHelper<T> implements Iterator<T>, Iterable<T>
    {

        private final CompletionService<T> service;
        private int numTasks;

        ExecutionHelper( final Executor executor )
        {
            this.service = new ExecutorCompletionService<T>(executor);
        }

        @Override
        public boolean hasNext()
        {
            return numTasks > 0;
        }

        public void submit( Callable<T> task )
        {
            this.service.submit(task);
            ++numTasks;
        }

        @Override
        public T next()
        {
            if (!this.hasNext())
            {
                throw new NoSuchElementException();
            }
            try
            {
                return service.take().get();
            }
            catch (InterruptedException e)
            {
                throw new ThreadInterruptedException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            finally
            {
                --numTasks;
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<T> iterator()
        {
            // use the shortcut here - this is only used in a privat context
            return this;
        }
    }
}
