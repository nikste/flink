package org.apache.flink.ml.feature_extraction

import breeze.linalg._
import breeze.numerics._
/**
 * Created by nikste on 7/1/15.
 */
class NeuralNet(weights:Array[DenseMatrix[Double]],learningRate : Double) {

    // constructor
    // add bias vector
    for(i <- 0 to weights.length - 1){
      weights(i) = DenseMatrix.horzcat( weights(i),DenseMatrix.rand[Double](weights(i).rows,1))
      println("weights(" + i + ") = \n" + weights(i))
    }
    
  
    
  def activation_function(in: DenseVector[Double]): DenseVector[Double] = {
      var out : DenseVector[Double] = breeze.numerics.exp(-in)
      out = out :+ 1.0
      var ones : breeze.linalg.DenseVector[Double] = breeze.linalg.DenseVector.ones[Double](out.length)
      out = 1.0 ./ (out)
      out
    }

    def printweights(): Unit ={
      for(i <- 0 to weights.length - 1){
        println("weights(" + i + "):\n" + weights(i))
      }
    }
  
    def train(input:DenseVector[Double],target:DenseVector[Double]): DenseVector[Double] = {//DenseVector[Double] ={
      
      var vectorSize = weights(0).rows

      // feed forward (ignoring bias vector)
      var inputWithBias = DenseVector.vertcat(input, DenseVector.ones[Double](1))
      
      var z_1 : DenseVector[Double] = weights(0) * inputWithBias // + bias
      var a_1 : DenseVector[Double] = activation_function(z_1)
      var a_1_withBias : DenseVector[Double] = DenseVector.vertcat(a_1, DenseVector.ones[Double](1))
      
      var z_2 : DenseVector[Double] = weights(1) * a_1_withBias // + bias
      var a_2 : DenseVector[Double] = activation_function(z_2)
      var output : DenseVector[Double] = a_2
      
      // backpropagation
      // 1. compute activations
      
      var d_output : DenseVector[Double] = -(target :- a_2) :* (a_2 :* ( - a_2 + 1d))
      // 
      var backpropagated_error : DenseVector[Double] = weights(1).t * d_output
      /*
      println("backpropagated_error:\n" + backpropagated_error)
      println("a_1\n" + a_1_withBias)
      */
      var d_hidden : DenseVector[Double] = backpropagated_error.slice(0,backpropagated_error.length-1) :* (a_1 :* ( - a_1 + 1d))
      
      // update weights:
      var delta_weight_1 = d_output * a_1_withBias.t //remove here with bias
      var delta_weight_0 = d_hidden * inputWithBias.t
      
      /*
      println("weights(1):\n" + weights(0).rows + "," + weights(0).cols)
      println("delta_weight_0:\n" + delta_weight_0.rows + "," + delta_weight_0.cols)
      println("d_hidden:  " + d_hidden.toDenseMatrix.rows + "," + d_hidden.toDenseMatrix.cols  )
      println("update:\n" +DenseMatrix.horzcat(delta_weight_0, d_hidden.toDenseMatrix.t))
      */
      weights(1) = weights(1) :-  learningRate * delta_weight_1//DenseMatrix.horzcat( delta_weight_1,   d_output.toDenseMatrix)
      
      weights(0) = weights(0) :- learningRate * delta_weight_0//DenseMatrix.horzcat(delta_weight_0, d_hidden.toDenseMatrix.t)
      
      var error = abs(a_2 - target)
      error
      /**
      // input -> hidden
      // for word2vec only use one column.
      //var hidden_net = DenseMatrix(Array.fill[Double](vectorSize)(0.0))

      /*for(i <- 0 to vectorSize - 1){
        hidden_net(i) = weights(0)(i,inIdx)
      }*/
      
      var hidden_net =  (input.t * weights(0)).t
      println("hidden_net:" + hidden_net)
      
      /*var hidden_act = DenseVector(Array.fill[Double](vectorSize)(0.0))
      for(i <- 0 to vectorSize - 1) {
        hidden_act(i) = hidden_net(i)//activation_function(hidden_net(i))
      }*/
      
      var hidden_act : DenseVector[Double] =  activation_function(hidden_net)
      println("hidden_act:" + hidden_act)
      
      
      // hidden -> output
      //var numOutputs = vocab.get(outIdx).codeLen
      var output_net  = ( weights(1) * hidden_act)
      println("output_net:" + output_net)

      var output_act : DenseVector[Double] = activation_function(output_net)
      println("output_act:" + output_act)
      

      var err : DenseVector[Double] = target - output_act
      println("target: "+target+" err:" + err)
      
      // backpropagation
      // for sigmoid output function
      var d_output : DenseVector[Double] = (- err :* (output_act :* (- output_act + 1d)))
      println("d_output:" + d_output)
      /*var oneminusactivation : DenseVector[Double]=  - output_act + 1d
      var derr : DenseVector[Double] = output_act :* oneminusactivation
      var neg_err : DenseVector[Double] = 0 - err
      var d_output : DenseVector[Double] = neg_err :*  derr
      */
      
      // derivative of activation function
      /*oneminusactivation = - hidden_act + 1d
      var d_act : DenseVector[Double] = hidden_act :* oneminusactivation
      var aux : DenseVector[Double] =  d_output * weights(0) 
      var d_hidden : DenseVector[Double]  = aux :* d_act
       */
      var d_act =  output_act :* (- output_act + 1d)
      
      println("d_act" + d_act)
      
      
      println("d_output : \n" + d_output)
      println("weights(0) : \n" + weights(0))
      var d_hidden : DenseMatrix[Double] = weights(0) * d_output.t//d_output.t * weights(0)
      
      println("d_hidden:" + d_hidden)
      
      
      
      // update weights:
      var updateTerm : DenseMatrix[Double] = (d_output.t * output_act).asInstanceOf[DenseMatrix[Double]] 
      weights(1) -= learningRate * updateTerm
      updateTerm = (d_hidden.t * hidden_act).asInstanceOf[DenseMatrix[Double]]
      weights(0) -= learningRate * updateTerm
      err
      /*var errgrad_output : DenseVector[Double] = - error :* (output_act .* ( 1 - output_act))
      
      
      var errgrad1 = DenseMatrix.zeros(vectorSize,numOutputs)
      for(netOutputIdx <- 0 to numOutputs - 1){
        var netOutputNum = vocab.get(outIdx).point(netOutputIdx)
        var target = vocab.get(outIdx).code(netOutputIdx)

        var output_net = 0.0
        for(i <- 0 to vectorSize - 1){
          output_net += layer1(netOutputNum,i) * hidden_act(i)
        }



        var output_act = activation_function(output_net)

        error += math.abs(output_act - target)

        var output_error_gradient = ( output_act - target) * (1 - output_act ) * output_act


        if(last_it){
          println("gt = " + target + "  is = " + output_act)
        }

        // backpropagation

        // layer1 update

        for(i <- 0 to vectorSize - 1){
          layer1(netOutputNum,i) -= learningRate * output_error_gradient * hidden_act(i)
          errgrad1(i,netOutputIdx) += output_error_gradient * layer1(netOutputNum,i)
        }

      }

      // layer0 update
      for(netOutputIdx <- 0 to numOutputs - 1){
        for(i <- 0 to vectorSize - 1){
          if(hidden_net(i) < 0.000000000001){
            hidden_net(i) = 0.000000000001
          }
          layer0(i,inIdx) += -learningRate * errgrad1(i,netOutputIdx) * hidden_net(i) * (1 - hidden_net(i))
        }
      }

      (layer0,layer1,error)*/
    **/
      }
}
