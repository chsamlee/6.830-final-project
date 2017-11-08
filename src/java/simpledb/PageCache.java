package simpledb;

import java.util.*;

/**
 * A thread-safe LRU pages for pages that is based on access order.
 */
public class PageCache {

    private final int maxSize;
    private final Map<PageId, Page> pages;
    private final ExposedLinkedList<PageId> accessOrder;
    private final Map<PageId, ExposedLinkedList<PageId>.Node> orderNodes;
    private final Map<PageId, Page> pagesToEvict;  // pages that were locked when they should be evicted

    public PageCache(int size) {
        this.maxSize = size;
        this.pages = new HashMap<>();
        this.accessOrder = new ExposedLinkedList<>();
        this.orderNodes = new HashMap<>();
        this.pagesToEvict = new HashMap<>();
    }

    /**
     * An immutable view of the pages.
     */
    public synchronized Collection<Page> pages() {
        // Note: if this is too slow, cache the immutable collection
        return Collections.unmodifiableCollection(pages.values());
    }

    /**
     * Evict a page that should have been evicted but was locked.
     * Returns the page.
     */
    public synchronized Page evictIfNotUsed(PageId pid) {
        Page page = pagesToEvict.remove(pid);
        return page;
    }

    /**
     * Checks if the pages contains a page.
     */
    public synchronized boolean contains(PageId pid) {
        return pages.containsKey(pid);
    }

    /**
     * Fetch a page from pages.
     */
    public synchronized Page get(PageId pid) {
        // first try to fetch from the to-be-evicted pages
        if (pagesToEvict.containsKey(pid)) {
            return pagesToEvict.get(pid);
        }
        // now try to fetch from main cache
        ExposedLinkedList<PageId>.Node node = orderNodes.get(pid);
        if (node == null) {
            return null;
        }
        accessOrder.pushToEnd(node);
        return pages.get(pid);
    }

    /**
     * Put a page in the pages, and possibly evict a page.
     * @param page the page to put in pages
     * @return the page that is evicted
     * @throws DbException if all pages are dirty
     */
    public synchronized Page put(Page page) throws DbException {
        PageId pid = page.getId();
        // page is already in cache: update to latest version
        if (pages.containsKey(pid)) {
            ExposedLinkedList<PageId>.Node node = orderNodes.get(pid);
            accessOrder.pushToEnd(node);
            pages.put(pid, page);
            return null;
        }
        // cache not full yet
        if (pages.size() + pagesToEvict.size() < maxSize) {
            putWithoutEvict(page);
            return null;
        }
        // evict the first least-recently-used page that is not locked
        while (accessOrder.head() != null) {
            PageId headPid = accessOrder.head().val();
            Page headPage = pages.get(headPid);
            if (Database.getBufferPool().getLockManager().pageIsLockedByWriter(headPid)) {
                pages.remove(headPid);
                accessOrder.pop();
                pagesToEvict.put(headPid, headPage);
            }
            else {
                remove(headPid);
                putWithoutEvict(page);
                return headPage;
            }
        }
        throw new DbException("All pages dirty");
    }

    /**
     * Add a page without evicting other pages.
     */
    private synchronized void putWithoutEvict(Page page) {
        PageId pid = page.getId();
        ExposedLinkedList<PageId>.Node node = accessOrder.add(pid);
        orderNodes.put(pid, node);
        pages.put(pid, page);
    }

    /**
     * Remove a page.
     */
    public synchronized Page remove(PageId pid) {
        ExposedLinkedList<PageId>.Node node = orderNodes.remove(pid);
        if (node == null) {
            return null;
        }
        accessOrder.remove(node);
        orderNodes.remove(pid);
        return pages.remove(pid);
    }

    /**
     * Remove multiple pages from the cache.
     */
    public synchronized void removeAll(Collection<PageId> pages) {
        for (PageId pid: pages) {
            remove(pid);
        }
    }

}
