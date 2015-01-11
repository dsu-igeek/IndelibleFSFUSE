/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * @Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.@
 */
package com.igeekinc.indelible.indeliblefs.fusefs;

import java.util.HashMap;

import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.luwak.inode.FUSEInodeManager;
import com.igeekinc.util.objectcache.LRUQueue;

public class IndelibleFSInodeManager extends FUSEInodeManager<IndelibleFSInode>
{
	protected HashMap<ObjectID, IndelibleFSInode>inodeByObjectIDMap = new HashMap<ObjectID, IndelibleFSInode>();
	protected LRUQueue<ObjectID, IndelibleFSInode>lruQueue = new LRUQueue<ObjectID, IndelibleFSInode>(50);
	public IndelibleFSInodeManager(IndelibleFSFUSEVolume parent)
	{
		super(parent);
	}

	@Override
	public boolean addInode(IndelibleFSInode addInode)
	{
		boolean returnValue;
		if (super.addInode(addInode))
		{
			synchronized(inodeByObjectIDMap)
			{
				ObjectID oid;

				oid = addInode.getNode().getObjectID();
				if (inodeByObjectIDMap.containsKey(oid))
				{
					returnValue = false;
				}
				else
				{
					inodeByObjectIDMap.put(oid, addInode);
					returnValue = true;
				}
			}
			if (!returnValue)
				super.removeInode(addInode);
		}
		else
		{
			returnValue = false;
		}
		return returnValue;
	}
	public IndelibleFSInode retrieveInodeForID(IndelibleFileNodeIF retrieveNode)
	{
		synchronized(map)	// We use map for the locking, not inodeByObjectIDMap
		{
			ObjectID retrieveID = retrieveNode.getObjectID();
			IndelibleFSInode returnNode = inodeByObjectIDMap.get(retrieveID);
			if (returnNode == null)
			{
				returnNode = lruQueue.get(retrieveID);
				if (returnNode != null)
					inodeByObjectIDMap.put(retrieveID, returnNode);
			}
			return returnNode;
		}
	}
	
	public void removeInode(IndelibleFSInode releaseNode)
	{
		synchronized(map)
		{
			super.removeInode(releaseNode);
			lruQueue.put(releaseNode.getNode().getObjectID(), releaseNode);
		}
	}
}
