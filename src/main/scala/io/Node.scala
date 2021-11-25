package io

import scala.collection.mutable

/**
  * trie node in AC automaton
  *
  * @param state   the index of node in trie
  * @param value   the part byte of string
  * @param goto    goto function
  * @param failure failure function
  * @param output  output function set[(key word, word property)]
  */
case class Node(state: Int,
                value: Byte,
                goto: mutable.Map[Byte, Int],
                var failure: Int,
                output: mutable.ArrayBuffer[(String, String)]) {

    override def toString: String = {
        state.toString + "," + value + "," + goto + "," + failure + "," + output
    }
}