#############################################################################
## A general-purpose doubly-linked list class that provides an interface
## modelled on Array.
##
## --------------------------------------------------------------------------
## Copyright (C) 2007, 2008, 2009 The Regents of the University of California.
## 
## This file is part of Marinda.
## 
## Marinda is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
## 
## Marinda is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
## 
## You should have received a copy of the GNU General Public License
## along with Marinda.  If not, see <http://www.gnu.org/licenses/>.
##
## $Id: list.rb,v 1.36 2009/04/02 22:21:59 youngh Exp $
#############################################################################

module Marinda

class List

  ASSERT = true

  if ASSERT
    def assert(value, msg)
      fail msg unless value
    end
  else
    def assert(value, msg); end
  end

  class Node
    attr_accessor :value, :prev, :next, :list

    def initialize(value, list)
      @value = value
      @prev = @next = nil
      @list = list
    end

    def active?
      @prev && @next
    end

    def inspect
      sprintf("\#<List::Node:%#x @value=%s @prev=%#x @next=%#x @list=%#x>",
	      object_id(), @value.to_s, @prev.object_id, @next.object_id,
	      @list.object_id)
    end

    def to_s
      "ListNode[#{@value.to_s}]"
    end
  end

  include Enumerable

  attr_reader :head
  protected :head

  def initialize(*values)
    @head = nil;  # circular doubly-linked list
    @length = 0;
    push(*values)
  end

  private #=================================================================

  # Inserts {values} before {node}. Returns newly created node for first value.
  def insert_before(node, *values)
    assert node.active?, "received deleted node"
    assert node.list == self, "received node of different list"
    assert @length > 0, "unexpected empty list"

    current = node
    values.reverse_each do |v|
      n = Node.new v, self
      n.prev = current.prev
      n.next = current
      current.prev.next = current.prev = n
      current = n
    end
    
    @head = current if @head == node
    @length += values.length
    current
  end

  # Deletes all nodes for which the supplied block yields true (on the
  # node value).  Returns true if nodes were deleted; false otherwise.
  #
  # This guarantees that nodes are yielded in order and only once.
  def delete_cond
    found = false

    node = @head
    while node
      node = find_matching_node(node) { |n| yield n.value }
      break unless node

      found = true
      node = unlink node
    end

    found
  end

  # External iterator: Returns the next node, starting *at* {node},
  # for which the block yields true (on the node value); returns nil if
  # no such node is found.
  #
  # To scan an entire list, start with node = find_matching_node(@head)
  # and repeatedly call node = find_matching_node(node) until node is nil.
  #
  # Use this external iterator (rather than each_node()) when you need
  # to modify the list (i.e., add/delete nodes) while iterating.
  #
  # This guarantees that nodes are yielded in order and only once.
  def find_matching_node(node)
    assert node.active?, "received deleted node"
    assert node.list == self, "received node of different list"
    assert @length > 0, "unexpected empty list"

    loop do
      return node if yield node
      return nil if node.next.equal? @head
      node = node.next
    end
    # NOT REACHED
  end

  # Unlinks {node} and returns its successor or nil if {node} is at list end.
  def unlink(node)
    assert node.active?, "attempt to delete already deleted node"
    assert node.list == self, "received node of different list"
    assert @length > 0, "attempt to delete node from empty list"

    if @length == 1
      @head = nil
      next_node = nil
    else # @length > 1
      next_node = (node.next.equal?(@head) ? nil : node.next)
      @head = node.next if @head.equal? node
      node.prev.next = node.next
      node.next.prev = node.prev
    end
    node.prev = node.next = nil
    node.list = nil

    @length -= 1
    next_node
  end

  # Mimic Array's typechecking of the 'other' argument, as follows:
  #
  #   >> l = List.new(1,2,3,4)
  #   >> l.to_ary => [1, 2, 3, 4]
  #   >> a = [1,2,3,4]
  #   >> a == l => false
  #   >> a === l => false
  #   >> a.eql?(l) => false
  def compare(other)
    return false unless other.kind_of? List 
    return false unless @length == other.length

    lhs = @head
    rhs = other.head
    loop do
      return false unless yield lhs.value, rhs.value
      return true if lhs.next.equal? @head

      lhs = lhs.next
      rhs = rhs.next
    end
    # NOT REACHED
  end

  def each_node
    return nil if @length == 0

    n = @head
    loop do
      yield n
      break if n.next.equal? @head
      n = n.next
    end
    self
  end

  def reverse_each_node
    return nil if @length == 0

    n = @head.prev
    loop do
      yield n
      break if n.equal? @head
      n = n.prev
    end
    self
  end

  public #==================================================================

  def List.[](*values)
    List.new(*values)
  end

  def <<(value)
    push value
    self
  end

  # Mimic Array's typechecking of the 'other' argument: throw a TypeError
  # unless the argument is a List.
  def <=>(other)
    other.kind_of? List or
      raise TypeError, "argument to <=> not List type" 

    retval = (@length <=> other.length)
    return retval unless retval == 0

    lhs = @head
    rhs = other.head
    loop do
      retval = (lhs.value <=> rhs.value)
      break if retval != 0 || lhs.next.equal?(@head)
      lhs = lhs.next
      rhs = rhs.next
    end
    retval
  end

  def ==(other)
    compare(other) { |v1, v2| v1 == v2 }
  end

  def ===(other)
    compare(other) { |v1, v2| v1 === v2 }
  end

  def clear
    @head = nil
    @length = 0
    self
  end

  def collect!
    each_node { |n| n.value = yield n.value }
    self
  end

  alias_method :map!, :collect!

  # Mimic Array's typechecking of the 'other' argument, as follows:
  #
  #   >> a = [5,6]
  #   >> l = List.new(1,2,3)
  #   >> l.respond_to? :to_ary => false  # commented out to_ary()
  #   >> a.concat(l)        # NOTE: doesn't use other.each()
  #   TypeError: can't convert List into Array
  def concat(other)
    unless other.kind_of? List
      if other.respond_to? :to_list
	other = other.to_list
      else
	raise TypeError, "can't convert #{other.class.to_s} to List" 
      end
    end

    other.each { |v| push v }
    self
  end
 
  # Mimic Array's behavior, as follows:
  #
  #    >> x = [4,5]
  #    >> y = [4,5]
  #    >> a = [[1,2,3], x]
  #    >> z = a.delete(y) => [4, 5]
  #    >> z.equal? x => false
  #    >> z.equal? y => true
  #
  # Support the case {value == nil}.
  #
  #    >> a = [1,2,nil,4]
  #    >> a.delete(nil) => nil
  #    >> a => [1, 2, 4]
  def delete(value)
    delete_cond { |v| v == value } ? value : (block_given? ? yield : nil)
  end

  def delete_if(&block)
    delete_cond(&block)
    self
  end

  def delete_node(node)
    node.kind_of? List::Node or
      raise TypeError, "argument to delete_node not List::Node type" 

    node.active? or
      raise ArgumentError, "argument to delete_node has already been deleted"

    node.list == self or
      raise ArgumentError, "argument to delete_node is from wrong list"

    unlink node
  end

  def each
    each_node { |n| yield n.value }
  end

  def empty?
    @length == 0
  end

  def eql?(other)
    return true if self.equal? other
    compare(other) { |v1, v2| v1.eql? v2 }
  end

  # This guarantees that nodes are yielded in order and only once.
  def find_all_nodes
    retval = []
    node = @head
    while node
      node = find_matching_node(node) { |n| yield n.value }
      break unless node

      retval << node
      node = (node.next.equal?(@head) ? nil : node.next)
    end
    retval
  end

  # This guarantees that nodes are yielded in order and only once.
  def find_node
    return nil if @length == 0
    find_matching_node(@head) { |n| yield n.value }
  end

  # XXX: Should this return Array or List for the multi-value case?
  # Mimic Array.first's behavior with respect to {count}.
  def first(count=nil)
    if count
      retval = []
      each_with_index do |v,i|
	break unless i < count
	retval << v
      end
      return retval
    else
      return (@length == 0 ? nil : @head.value)
    end
  end

  def first_node
    @length == 0 ? nil : @head
  end

  def include?(value)
    each { |v| return true if v == value }
    false
  end

  def inspect
    sprintf("\#<List:%#x @head=%#x @length=%d [%s]>", object_id(),
	    @head.object_id, @length, join(", "))
  end

  # Inserts {values} before {node}. Returns newly created node for first value.
  def insert(node, *values)
    node.kind_of? List::Node or
      raise TypeError, "node argument to insert not List::Node type" 

    node.active? or
      raise ArgumentError, "node argument to insert not active"

    node.list == self or
      raise ArgumentError, "node argument to insert is from wrong list"

    insert_before node, *values
  end

  def join(separator="")
    n = 0
    retval = ""
    each do |v|
      retval << separator if n > 0
      retval << v.to_s
      n += 1
    end
    retval
  end

  # XXX: Should this return Array or List for the multi-value case?
  # Mimic Array.last's behavior with respect to {count}.
  def last(count=nil)
    if count
      count >= 0 or
	raise ArgumentError, "count argument must be >= 0"

      i = (count < @length ? count : @length)
      retval = []
      reverse_each do |v|
	break if i == 0
	i -= 1; retval[i] = v
      end
      return retval
    else
      return (@length == 0 ? nil : @head.prev.value)
    end
  end

  def last_node
    @length == 0 ? nil : @head.prev
  end

  def length
    @length
  end

  alias_method :size, :length

  def pop
    if @length == 0
      return nil
    else
      n = @head.prev

      @length -= 1
      if @length == 0
	@head = nil
      else
	@head.prev = n.prev
	n.prev.next = @head
      end

      return n.value
    end
  end

  def push(*values)
    values.each do |v|
      n = Node.new v, self

      if @head
	n.prev = @head.prev
	n.next = @head
	@head.prev.next = n
	@head.prev = n
      elsif
	n.prev = n.next = n
	@head = n
      end

      @length += 1
    end
    self
  end

  def reject!(&block)
    delete_cond(&block) ? self : nil
  end

  # NOTE: Don't try to invoke other.to_list (Array.replace doesn't call
  #       other.to_a).
  def replace(other)
    other.kind_of? List or
      raise TypeError, "can't convert #{other.class.to_s} to List" 

    clear
    other.each { |v| push v }
    self
  end

  def reverse
    retval = List.new
    reverse_each { |v| retval << v }
    retval
  end

  def reverse!
    return self if @length <= 1

    node = @head
    loop do
      next_node = node.next
      node.prev, node.next = node.next, node.prev
      node = next_node
      break if node.equal? @head
    end
    @head = @head.next
    self
  end

  def reverse_each
    reverse_each_node { |n| yield n.value }
  end

  def shift
    return nil if @length == 0
    @head = @head.next    # move head element to end of list for popping
    pop
  end

  def to_a
    retval = Array.new
    each { |v| retval.push v }
    retval
  end

  def to_list
    self
  end

  def to_s
    "List[" + join(", ") + "]"
  end

  def uniq
    retval = List.new
    values = {}
    each do |v|
      unless values.member? v
	values[v] = 1
	retval << v 
      end
    end
    retval
  end

  def uniq!
    values = {}
    delete_cond { |v| values.member?(v) ? true : (values[v] = 1; false) }
    self
  end

  # Mimic Array's behavior, as follows:
  #
  #   >> a = [1,2,3]
  #   >> a.unshift(5,6,7) => [5, 6, 7, 1, 2, 3]
  def unshift(*values)
    new_head = nil
    values.each do |v|
      push v
      new_head ||= @head.prev  # first unshifted element
    end
    @head = (new_head ? new_head : @head)  # careful: handle empty *values
    self
  end

end

end
