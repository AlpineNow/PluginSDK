/*
 * Copyright (c) 2015 Alpine Data Labs
 * All rights reserved.
 */
package com.alpine.model.pack.ml

import com.alpine.model.ClusteringRowModel
import com.alpine.model.export.pfa.modelconverters.KMeansPFAConverter
import com.alpine.model.export.pfa.{PFAConverter, PFAConvertible}
import com.alpine.model.pack.util.TransformerUtil
import com.alpine.plugin.core.io.ColumnDef
import com.alpine.sql.SQLGenerator
import com.alpine.transformer.ClusteringTransformer
import com.alpine.transformer.sql.{ClusteringModelSQLExpressions, ClusteringSQLTransformer, ColumnName, ColumnarSQLExpression}

/**
 * A model representing results of the K-Means clustering algorithm.
 * Each cluster is a vector in a fixed dimensional space, and the transformer
 * assigns the input row to its closest cluster using the Euclidean (L2) distance.
 *
 * The clusters should all be of the same dimension, which should be equal to the
 * number of input features.
 *
 * @param clusters The clusters of the model. These should have distinct names.
 * @param inputFeatures A seq of numeric feature descriptions describing the input
 *                      to the model.
 * @param identifier
 */
case class KMeansModel(clusters: Seq[ClusterInfo],
                       inputFeatures: Seq[ColumnDef],
                       override val identifier: String = "")
  extends ClusteringRowModel with PFAConvertible {
  override def classLabels: Seq[String] = clusters.map(_.name)

  override def transformer = KMeansTransformer(this)

  override def sqlTransformer(sqlGenerator: SQLGenerator) = Some(new KMeansSQLTransformer(this, sqlGenerator))

  override def getPFAConverter: PFAConverter = new KMeansPFAConverter(this)
}

/**
 * Representation of a single cluster.
 *
 * We use java.lang.Double for the type of the numeric values, because the scala Double type information
 * is lost by scala/Gson and the deserialization fails badly for edge cases (e.g. Double.NaN).
 *
 * @param name Name used to distinguish this cluster from others in the same model.
 * @param centroid A vector representation of the cluster in orthogonal coordinates.
 */
case class ClusterInfo(name: String, centroid: Seq[java.lang.Double])

case class KMeansTransformer(model: KMeansModel) extends ClusteringTransformer {

  val dim = model.clusters.head.centroid.length
  val numClasses = model.clusters.size
  // Use Arrays for faster indexing.
  private val clustersArray: Array[Array[Double]] = model.clusters.map(c => TransformerUtil.javaDoubleSeqToArray(c.centroid)).toArray
  // Reuse of this means that the scoring method is not thread safe.
  private val rowAsDoubleArray = Array.ofDim[Double](model.transformationSchema.inputFeatures.length)

  override def scoreDistances(row: Row): Array[Double] = {
    TransformerUtil.fillRowToDoubleArray(row, rowAsDoubleArray)
    val distances = Array.ofDim[Double](numClasses)
    var i = 0
    while (i < numClasses) {
      distances(i) = L2Distance(rowAsDoubleArray, clustersArray(i))
      i += 1
    }
    distances
  }

  // Treats row and cluster as vectors (must both be of length dim), and finds the L2 (Euclidean) distance between them.
  private def L2Distance(row: Array[Double], cluster: Array[Double]): Double = {
    var d = 0d
    var i = 0
    while(i < dim) {
      val diff = row(i) - cluster(i)
      d += diff * diff
      i += 1
    }
    math.sqrt(d)
  }

  /**
   * The result must always return the labels in the order specified here.
 *
   * @return The class labels in the order that they will be returned by the result.
   */
  lazy val classLabels: Seq[String] = model.classLabels

}

class KMeansSQLTransformer(val model: KMeansModel, sqlGenerator: SQLGenerator) extends ClusteringSQLTransformer {

  override def getClusteringSQL: ClusteringModelSQLExpressions = {

    val clusterExpressions = model.clusters.map(cluster => {
      val s = (cluster.centroid zip inputColumnNames).map {
        case (coordinate, name) =>
          val diff = coordinate + " - " + name.escape(sqlGenerator)
          "(" + diff + ") * (" + diff + ")"
      }.mkString(" + ")
      ColumnarSQLExpression("POWER(" + s + ", 0.5)")
    })

    val temporaryDistanceNames = clusterExpressions.indices.map(i => ColumnName("DIST_" + i))

    ClusteringModelSQLExpressions(
      model.clusters.map(cluster => cluster.name) zip temporaryDistanceNames,
      Seq(clusterExpressions zip temporaryDistanceNames),
      sqlGenerator
    )
  }

}
