/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2015.
 */

package x10.util.resilient.store;

import x10.util.resilient.localstore.Cloneable;
import x10.util.resilient.localstore.ResilientStore;
import x10.util.resilient.localstore.ResilientNativeMap;
import x10.util.resilient.localstore.Tx;
import x10.util.resilient.PlaceManager.ChangeDescription;
import x10.util.HashMap;
import x10.util.resilient.localstore.TxResult;

public class NativeStore[V]{V haszero, V <: Cloneable} extends Store[V] {
    val store:ResilientStore;
      val map:ResilientNativeMap;

      def this(name:String, activePlaces:PlaceGroup) {
          val immediateRecovery = false;
          store = ResilientStore.make(activePlaces, immediateRecovery);
          map = store.makeMap("_map_" + name);
      }

      public def get(key:String) = map.get(key) as V;

      public def set(key:String, value:V) {
          map.set(key, value);
      }
  
      public def setAll(pairs:HashMap[String,V]) {
          val tmp = new HashMap[String,Cloneable]();
          val iter = pairs.keySet().iterator();
          while (iter.hasNext()) {
              val k = iter.next();
              val v = pairs.getOrThrow(k) as Cloneable;
              tmp.put(k,v);
          }
          map.setAll(tmp);
      } 

      public def set2(key:String, value:V, place:Place, key2:String, value2:V) {
          map.set2(key, value, place, key2, value2);
      }

      public def getActivePlaces() = store.getActivePlaces();

      // update for changes in the active PlaceGroup
      public def updateForChangedPlaces(changes:ChangeDescription):void {
          store.updateForChangedPlaces(changes);
      }
  
      public def executeTransaction(members:PlaceGroup, closure:(Tx)=>Any):TxResult {
          return map.executeTransaction(members, closure);
      }
}
