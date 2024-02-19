#[cfg(test)]
mod tests;

use std::{cell::RefCell, ops::Deref, rc::Rc};

/// An interval-value map, which support operations on dynamic sets of intervals.
pub struct IntervalMap<T, V> {
    /// Root of the interval tree
    root: NodeRef<T, V>,
    /// Sentinel node, used to simplify boundary checkes
    sentinel: NodeRef<T, V>,
    /// Number of elements in the map
    len: usize,
}

impl<T, V> IntervalMap<T, V>
where
    T: Ord + Clone,
{
    /// Creates an empty `IntervalMap`
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        let sentinel = Self::new_sentinel();
        Self {
            root: sentinel.clone_rc(),
            sentinel,
            len: 0,
        }
    }

    /// Inserts a interval-value pair into the map.
    #[inline]
    pub fn insert(&mut self, interval: Interval<T>, value: V) -> Option<V> {
        let z = self.new_node(interval, value);
        self.insert_inner(z)
    }

    /// Gets the given key's corresponding entry in the map for in-place manipulation.
    #[inline]
    pub fn entry(&mut self, interval: Interval<T>) -> Entry<'_, T, V> {
        match self.search_exact(&interval) {
            Some(node) => Entry::Occupied(OccupiedEntry { node }),
            None => Entry::Vacant(VacantEntry {
                map_ref: self,
                interval,
            }),
        }
    }

    /// Removes a interval from the map, returning the value at the interval if the interval
    /// was previously in the map.
    #[inline]
    pub fn remove(&mut self, interval: &Interval<T>) -> Option<V> {
        if let Some(node) = self.search_exact(interval) {
            self.remove_inner(&node);
            return Some(node.take_value());
        }
        None
    }

    /// Checks if an interval in the map overlaps with the given interval.
    #[inline]
    pub fn overlap(&self, interval: &Interval<T>) -> bool {
        let node = self.search(interval);
        !node.is_sentinel()
    }

    /// Finds all intervals in the map that overlaps with the given interval.
    #[inline]
    pub fn find_all_overlap(&self, interval: &Interval<T>) -> Vec<Interval<T>> {
        Self::find_all_overlap_inner(&self.root, interval)
    }

    /// Returns the number of elements in the map.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if the map contains no elements.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl<T, V> Default for IntervalMap<T, V>
where
    T: Ord + Clone,
{
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<T, V> IntervalMap<T, V>
where
    T: Clone,
{
    /// Creates a new sentinel node
    fn new_sentinel() -> NodeRef<T, V> {
        Node {
            interval: None,
            value: None,
            max: None,
            left: None,
            right: None,
            parent: None,
            color: Color::Black,
        }
        .into_ref()
    }

    /// Creates a new tree node
    fn new_node(&self, interval: Interval<T>, value: V) -> NodeRef<T, V> {
        Node {
            max: Some(interval.high.clone()),
            interval: Some(interval),
            value: Some(value),
            left: Some(self.sentinel.clone_rc()),
            right: Some(self.sentinel.clone_rc()),
            parent: Some(self.sentinel.clone_rc()),
            color: Color::Red,
        }
        .into_ref()
    }
}

impl<T, V> IntervalMap<T, V>
where
    T: Ord + Clone,
{
    /// Inserts a node into the tree.
    fn insert_inner(&mut self, z: NodeRef<T, V>) -> Option<V> {
        let mut y = self.sentinel.clone_rc();
        let mut x = self.root.clone_rc();
        while !x.is_sentinel() {
            y = x.clone_rc();
            if z.interval(|zi| y.interval(|yi| zi == yi)) {
                let old_value = y.set_value(z.take_value());
                return Some(old_value);
            }
            if z.interval(|zi| x.interval(|xi| zi < xi)) {
                x = x.left_owned();
            } else {
                x = x.right_owned();
            }
        }
        z.set_parent(y.clone_rc());
        if y.is_sentinel() {
            self.root = z.clone_rc();
        } else {
            if z.interval(|zi| y.interval(|yi| zi < yi)) {
                y.set_left(z.clone_rc());
            } else {
                y.set_right(z.clone_rc());
            }
            Self::update_max_bottom_up(&y);
        }
        z.set_color(Color::Red);

        self.insert_fixup(z.clone_rc());

        self.len = self.len.wrapping_add(1);
        None
    }

    /// Removes a node from the tree.
    fn remove_inner(&mut self, z: &NodeRef<T, V>) {
        let mut y = z.clone_rc();
        let mut y_orig_color = y.color();
        let x;
        if z.left(NodeRef::is_sentinel) {
            x = z.right_owned();
            self.transplant(z, &x);
        } else if z.right(NodeRef::is_sentinel) {
            x = z.left_owned();
            self.transplant(z, &x);
        } else {
            y = Self::tree_minimum(z.right_owned());
            y_orig_color = y.color();
            x = y.right_owned();
            if y.parent(|p| p == z) {
                x.set_parent(y.clone_rc());
            } else {
                self.transplant(&y, &x);
                y.set_right(z.right_owned());
                y.right(|r| r.set_parent(y.clone_rc()));
            }
            self.transplant(z, &y);
            y.set_left(z.left_owned());
            y.left(|l| l.set_parent(y.clone_rc()));
            y.set_color(z.color());
            Self::update_max_bottom_up(&y);
        }

        if matches!(y_orig_color, Color::Black) {
            self.remove_fixup(x);
        }

        self.len = self.len.wrapping_sub(1);
    }

    /// Finds all intervals in the map that overlaps with the given interval.
    fn find_all_overlap_inner(x: &NodeRef<T, V>, i: &Interval<T>) -> Vec<Interval<T>> {
        let mut list = vec![];
        if x.interval(|xi| xi.overlap(i)) {
            list.push(x.interval(Clone::clone));
        }
        if !x.left(NodeRef::is_sentinel) && x.left(|l| l.max(|m| *m >= i.low)) {
            list.extend(Self::find_all_overlap_inner(&x.left_owned(), i));
        }
        if !x.right(NodeRef::is_sentinel)
            && x.interval(|xi| xi.low <= i.high)
            && x.right(|r| r.max(|m| *m >= i.low))
        {
            list.extend(Self::find_all_overlap_inner(&x.right_owned(), i));
        }
        list
    }

    /// Search for an interval that overlaps with the given interval.
    fn search(&self, interval: &Interval<T>) -> NodeRef<T, V> {
        let mut x = self.root.clone_rc();
        while !x.is_sentinel() && x.interval(|xi| !xi.overlap(interval)) {
            if !x.left(NodeRef::is_sentinel) && x.left(|l| l.max(|lm| lm >= &interval.low)) {
                x = x.left_owned();
            } else {
                x = x.right_owned();
            }
        }
        x
    }

    /// Search for the node with exact the given interval
    fn search_exact(&self, interval: &Interval<T>) -> Option<NodeRef<T, V>> {
        let mut x = self.root.clone_rc();
        while !x.is_sentinel() {
            if x.interval(|xi| xi == interval) {
                return Some(x);
            }
            if x.max(|max| max < &interval.high) {
                return None;
            }
            if x.interval(|xi| xi > interval) {
                x = x.left_owned();
            } else {
                x = x.right_owned();
            }
        }
        None
    }

    /// Restores red-black tree properties after an insert.
    fn insert_fixup(&mut self, mut z: NodeRef<T, V>) {
        while z.parent(NodeRef::is_red) {
            if z.grand_parent(NodeRef::is_sentinel) {
                break;
            }
            if z.parent(Self::is_left_child) {
                let y = z.grand_parent(NodeRef::right_owned);
                if y.is_red() {
                    z.parent(NodeRef::set_color_fn(Color::Black));
                    y.set_color(Color::Black);
                    z.grand_parent(NodeRef::set_color_fn(Color::Red));
                    z = z.grand_parent_owned();
                } else {
                    if Self::is_right_child(&z) {
                        z = z.parent_owned();
                        self.left_rotate(&z);
                    }
                    z.parent(NodeRef::set_color_fn(Color::Black));
                    z.grand_parent(NodeRef::set_color_fn(Color::Red));
                    z.grand_parent(|gp| self.right_rotate(gp));
                }
            } else {
                let y = z.grand_parent(NodeRef::left_owned);
                if y.is_red() {
                    z.parent(NodeRef::set_color_fn(Color::Black));
                    y.set_color(Color::Black);
                    z.grand_parent(NodeRef::set_color_fn(Color::Red));
                    z = z.grand_parent_owned();
                } else {
                    if Self::is_left_child(&z) {
                        z = z.parent_owned();
                        self.right_rotate(&z);
                    }
                    z.parent(NodeRef::set_color_fn(Color::Black));
                    z.grand_parent(NodeRef::set_color_fn(Color::Red));
                    z.grand_parent(|gp| self.left_rotate(gp));
                }
            }
        }
        self.root.set_color(Color::Black);
    }

    /// Restores red-black tree properties after a remove.
    fn remove_fixup(&mut self, mut x: NodeRef<T, V>) {
        while x != self.root && x.is_black() {
            let mut w;
            if Self::is_left_child(&x) {
                w = x.parent(NodeRef::right_owned);
                if w.is_red() {
                    w.set_color(Color::Black);
                    x.parent(NodeRef::set_color_fn(Color::Red));
                    x.parent(|p| self.left_rotate(p));
                    w = x.parent(NodeRef::right_owned);
                }
                if w.is_sentinel() {
                    break;
                }
                if w.left(NodeRef::is_black) && w.right(NodeRef::is_black) {
                    w.set_color(Color::Red);
                    x = x.parent_owned();
                } else {
                    if w.right(NodeRef::is_black) {
                        w.left(NodeRef::set_color_fn(Color::Black));
                        w.set_color(Color::Red);
                        self.right_rotate(&w);
                        w = x.parent(NodeRef::right_owned);
                    }
                    w.set_color(x.parent(NodeRef::color));
                    x.parent(NodeRef::set_color_fn(Color::Black));
                    w.right(NodeRef::set_color_fn(Color::Black));
                    x.parent(|p| self.left_rotate(p));
                    x = self.root.clone_rc();
                }
            } else {
                w = x.parent(NodeRef::left_owned);
                if w.is_red() {
                    w.set_color(Color::Black);
                    x.parent(NodeRef::set_color_fn(Color::Red));
                    x.parent(|p| self.right_rotate(p));
                    w = x.parent(NodeRef::left_owned);
                }
                if w.is_sentinel() {
                    break;
                }
                if w.left(NodeRef::is_black) && w.right(NodeRef::is_black) {
                    w.set_color(Color::Red);
                    x = x.parent_owned();
                } else {
                    if w.left(NodeRef::is_black) {
                        w.right(NodeRef::set_color_fn(Color::Black));
                        w.set_color(Color::Red);
                        self.left_rotate(&w);
                        w = x.parent(NodeRef::left_owned);
                    }
                    w.set_color(x.parent(NodeRef::color));
                    x.parent(NodeRef::set_color_fn(Color::Black));
                    w.left(NodeRef::set_color_fn(Color::Black));
                    x.parent(|p| self.right_rotate(p));
                    x = self.root.clone_rc();
                }
            }
        }
        x.set_color(Color::Black);
    }

    /// Binary tree left rotate.
    fn left_rotate(&mut self, x: &NodeRef<T, V>) {
        if x.right(NodeRef::is_sentinel) {
            return;
        }
        let y = x.right_owned();
        x.set_right(y.left_owned());
        if !y.left(NodeRef::is_sentinel) {
            y.left(|l| l.set_parent(x.clone_rc()));
        }

        self.replace_parent(x, &y);
        y.set_left(x.clone_rc());

        Self::rotate_update_max(x, &y);
    }

    /// Binary tree right rotate.
    fn right_rotate(&mut self, x: &NodeRef<T, V>) {
        if x.left(NodeRef::is_sentinel) {
            return;
        }
        let y = x.left_owned();
        x.set_left(y.right_owned());
        if !y.right(NodeRef::is_sentinel) {
            y.right(|r| r.set_parent(x.clone_rc()));
        }

        self.replace_parent(x, &y);
        y.set_right(x.clone_rc());

        Self::rotate_update_max(x, &y);
    }

    /// Replaces parent during a rotation.
    fn replace_parent(&mut self, x: &NodeRef<T, V>, y: &NodeRef<T, V>) {
        y.set_parent(x.parent_owned());
        if x.parent(NodeRef::is_sentinel) {
            self.root = y.clone_rc();
        } else if x.parent(|p| p.left(|l| l.eq(x))) {
            x.parent(|p| p.set_left(y.clone_rc()));
        } else {
            x.parent(|p| p.set_right(y.clone_rc()));
        }
        x.set_parent(y.clone_rc());
    }

    /// Updates the max value after a rotation.
    fn rotate_update_max(x: &NodeRef<T, V>, y: &NodeRef<T, V>) {
        y.set_max(x.max_owned());
        let mut max = x.interval(|i| i.high.clone());
        if !x.left(NodeRef::is_sentinel) {
            max = max.max(x.left(NodeRef::max_owned));
        }
        if !x.right(NodeRef::is_sentinel) {
            max = max.max(x.right(NodeRef::max_owned));
        }
        x.set_max(max);
    }

    /// Updates the max value towards the root
    fn update_max_bottom_up(x: &NodeRef<T, V>) {
        let mut p = x.clone_rc();
        while !p.is_sentinel() {
            p.set_max(p.interval(|i| i.high.clone()));
            Self::max_from(&p, &p.left_owned());
            Self::max_from(&p, &p.right_owned());
            p = p.parent_owned();
        }
    }

    /// Updates a nodes value from a child node.
    fn max_from(x: &NodeRef<T, V>, c: &NodeRef<T, V>) {
        if !c.is_sentinel() && x.max(|xm| c.max(|cm| xm < cm)) {
            x.set_max(c.max_owned());
        }
    }

    /// Finds the node with the minimum interval.
    fn tree_minimum(mut x: NodeRef<T, V>) -> NodeRef<T, V> {
        loop {
            let left = x.left_owned();
            if left.is_sentinel() {
                return x;
            }
            x = left;
        }
    }

    /// Replaces one subtree as a child of its parent with another subtree.
    fn transplant(&mut self, u: &NodeRef<T, V>, v: &NodeRef<T, V>) {
        if u.parent(NodeRef::is_sentinel) {
            self.root = v.clone_rc();
        } else {
            if u.parent(|p| p.left(|l| l == u)) {
                u.parent(|p| p.set_left(v.clone_rc()));
            } else {
                u.parent(|p| p.set_right(v.clone_rc()));
            }
            u.parent(Self::update_max_bottom_up);
        }
        v.set_parent(u.parent_owned());
    }

    /// Checks if a node is a left child of its parent.
    fn is_left_child(node: &NodeRef<T, V>) -> bool {
        node.parent(|p| p.left(|l| l.eq(node)))
    }

    /// Checks if a node is a right child of its parent.
    fn is_right_child(node: &NodeRef<T, V>) -> bool {
        node.parent(|p| p.right(|r| r.eq(node)))
    }
}

impl<T, V> std::fmt::Debug for IntervalMap<T, V> {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IntervalMap")
            .field("len", &self.len)
            .finish()
    }
}

// TODO: better typed `Node`
/// Node of the interval tree
struct Node<T, V> {
    /// Interval of the node
    interval: Option<Interval<T>>,
    /// Value of the node
    value: Option<V>,
    /// Max value of the sub-tree of the node
    max: Option<T>,

    /// Left children
    left: Option<NodeRef<T, V>>,
    /// Right children
    right: Option<NodeRef<T, V>>,
    /// Parent
    parent: Option<NodeRef<T, V>>,
    /// Color of the node
    color: Color,
}

#[allow(clippy::missing_docs_in_private_items, clippy::unwrap_used)]
impl<T, V> Node<T, V> {
    fn into_ref(self) -> NodeRef<T, V> {
        NodeRef(Rc::new(RefCell::new(self)))
    }

    fn color(&self) -> Color {
        self.color
    }

    fn interval(&self) -> &Interval<T> {
        self.interval.as_ref().unwrap()
    }

    fn max(&self) -> &T {
        self.max.as_ref().unwrap()
    }

    fn left(&self) -> &NodeRef<T, V> {
        self.left.as_ref().unwrap()
    }

    fn right(&self) -> &NodeRef<T, V> {
        self.right.as_ref().unwrap()
    }

    fn parent(&self) -> &NodeRef<T, V> {
        self.parent.as_ref().unwrap()
    }

    fn left_owned(&self) -> NodeRef<T, V> {
        self.left.as_ref().unwrap().clone_rc()
    }

    fn right_owned(&self) -> NodeRef<T, V> {
        self.right.as_ref().unwrap().clone_rc()
    }

    fn parent_owned(&self) -> NodeRef<T, V> {
        self.parent.as_ref().unwrap().clone_rc()
    }

    fn set_color(&mut self, color: Color) {
        self.color = color;
    }
}

/// `RefCell` reference to `Node`
struct NodeRef<T, V>(Rc<RefCell<Node<T, V>>>);

impl<T, V> Deref for NodeRef<T, V> {
    type Target = Rc<RefCell<Node<T, V>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, V> PartialEq for NodeRef<T, V> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(self, other)
    }
}

impl<T, V> Eq for NodeRef<T, V> {}

#[allow(clippy::missing_docs_in_private_items)]
impl<T, V> NodeRef<T, V> {
    fn clone_rc(&self) -> Self {
        Self(Rc::clone(self))
    }
}

#[allow(clippy::missing_docs_in_private_items, clippy::unwrap_used)]
impl<T, V> NodeRef<T, V> {
    fn is_sentinel(&self) -> bool {
        self.borrow().interval.is_none()
    }

    fn is_black(&self) -> bool {
        matches!(self.borrow().color, Color::Black)
    }

    fn is_red(&self) -> bool {
        matches!(self.borrow().color, Color::Red)
    }

    fn color(&self) -> Color {
        self.borrow().color()
    }

    fn max_owned(&self) -> T
    where
        T: Clone,
    {
        self.borrow().max().clone()
    }

    fn left_owned(&self) -> NodeRef<T, V> {
        self.borrow().left_owned()
    }

    fn right_owned(&self) -> NodeRef<T, V> {
        self.borrow().right_owned()
    }

    fn parent_owned(&self) -> NodeRef<T, V> {
        self.borrow().parent_owned()
    }

    fn grand_parent_owned(&self) -> NodeRef<T, V> {
        self.borrow().parent().parent_owned()
    }

    fn take_value(self) -> V {
        self.borrow_mut().value.take().unwrap()
    }

    fn set_value(&self, new_value: V) -> V {
        self.borrow_mut().value.replace(new_value).unwrap()
    }

    fn set_color(&self, color: Color) {
        self.borrow_mut().set_color(color);
    }

    fn set_color_fn(color: Color) -> impl FnOnce(&NodeRef<T, V>) {
        move |node: &NodeRef<T, V>| {
            node.borrow_mut().set_color(color);
        }
    }

    fn set_right(&self, node: NodeRef<T, V>) {
        let _ignore = self.borrow_mut().right.replace(node);
    }

    fn set_max(&self, max: T) {
        let _ignore = self.borrow_mut().max.replace(max);
    }

    fn set_left(&self, node: NodeRef<T, V>) {
        let _ignore = self.borrow_mut().left.replace(node);
    }

    fn set_parent(&self, node: NodeRef<T, V>) {
        let _ignore = self.borrow_mut().parent.replace(node);
    }
}

#[allow(clippy::missing_docs_in_private_items)]
impl<T, V> NodeRef<T, V> {
    fn max<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&T) -> R,
    {
        op(self.borrow().max())
    }

    fn interval<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&Interval<T>) -> R,
    {
        op(self.borrow().interval())
    }

    fn left<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&NodeRef<T, V>) -> R,
    {
        op(self.borrow().left())
    }

    fn right<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&NodeRef<T, V>) -> R,
    {
        op(self.borrow().right())
    }

    fn parent<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&NodeRef<T, V>) -> R,
    {
        op(self.borrow().parent())
    }

    fn grand_parent<F, R>(&self, op: F) -> R
    where
        F: FnOnce(&NodeRef<T, V>) -> R,
    {
        let mut grand_parent = self.borrow().parent().borrow().parent_owned();
        op(&mut grand_parent)
    }
}

/// The color of the node
#[derive(Debug, Clone, Copy)]
enum Color {
    /// Red node
    Red,
    /// Black node
    Black,
}

/// The Interval stored in `IntervalMap`
/// Represents the interval [low, high]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval<T> {
    /// Low value
    low: T,
    /// high value
    high: T,
}

impl<T: Ord> Interval<T> {
    /// Creates a new `Interval`
    ///
    /// # Panics
    ///
    /// This method panics when low is greater than high
    #[inline]
    pub fn new(low: T, high: T) -> Self {
        assert!(low <= high, "invalid range");
        Self { low, high }
    }

    /// Checks if self overlaps with other interval
    fn overlap(&self, other: &Self) -> bool {
        self.high >= other.low && other.high >= self.low
    }
}

/// A view into a single entry in a map, which may either be vacant or occupied.
#[allow(missing_debug_implementations, clippy::exhaustive_enums)]
pub enum Entry<'a, T, V> {
    /// An occupied entry.
    Occupied(OccupiedEntry<T, V>),
    /// A vacant entry.
    Vacant(VacantEntry<'a, T, V>),
}

/// A view into an occupied entry in a `IntervalMap`.
/// It is part of the [`Entry`] enum.
#[allow(missing_debug_implementations)]
pub struct OccupiedEntry<T, V> {
    /// The entry node
    node: NodeRef<T, V>,
}

/// A view into a vacant entry in a `IntervalMap`.
/// It is part of the [`Entry`] enum.
#[allow(missing_debug_implementations)]
pub struct VacantEntry<'a, T, V> {
    /// Mutable reference to the map
    map_ref: &'a mut IntervalMap<T, V>,
    /// The interval of this entry
    interval: Interval<T>,
}

impl<T, V> Entry<'_, T, V>
where
    T: Ord + Clone,
{
    /// Ensures a value is in the entry by inserting the default if empty, and returns
    /// a mutable reference to the value in the entry.
    #[inline]
    pub fn or_insert(self, default: V) {
        if let Entry::Vacant(entry) = self {
            let _ignore = entry.map_ref.insert(entry.interval, default);
        }
    }

    /// Provides in-place mutable access to an occupied entry before any
    /// potential inserts into the map.
    ///
    /// # Panics
    ///
    /// This method panics when the node is a sentinel node
    #[allow(clippy::unwrap_used)]
    #[inline]
    #[must_use]
    pub fn and_modify<F>(self, f: F) -> Self
    where
        F: FnOnce(&mut V),
    {
        match self {
            Entry::Occupied(entry) => {
                f(entry.node.borrow_mut().value.as_mut().unwrap());
                Self::Occupied(entry)
            }
            Entry::Vacant(entry) => Self::Vacant(entry),
        }
    }
}
