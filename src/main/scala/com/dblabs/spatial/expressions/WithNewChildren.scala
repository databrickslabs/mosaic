package com.dblabs.spatial.expressions

import org.apache.spark.sql.catalyst.expressions.Expression

trait WithNewChildren extends Expression { self: Product =>

    /** Number of leading constructor args that are Expressions */
    private def childCount: Int = children.size

    /** Pull out the first N fields as children */
    override def children: Seq[Expression] = productIterator.take(childCount).map(_.asInstanceOf[Expression]).toSeq

    /**
      * Rebuild via the primary constructor: newChildren ++ the rest of the
      * fields
      */
    override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
        require(newChildren.size == childCount, s"Expected $childCount children, got ${newChildren.size}")

        // Everything after the first N fields
        val tailArgs: Seq[AnyRef] = productIterator.drop(childCount).map(_.asInstanceOf[AnyRef]).toSeq

        // Find the case-class ctor whose param count matches productArity
        val ctor = self.getClass.getDeclaredConstructors
            .find(_.getParameterCount == productArity)
            .getOrElse(
              throw new IllegalStateException(
                s"No constructor with $productArity params on ${self.getClass.getName}"
              )
            )
        ctor.setAccessible(true)

        // Build an Array[AnyRef] of (newChildren ++ tailArgs)
        val args: Array[AnyRef] = (newChildren.map(_.asInstanceOf[AnyRef]) ++ tailArgs).toArray

        // Invoke and cast back to Expression
        ctor.newInstance(args: _*).asInstanceOf[Expression]
    }

}
