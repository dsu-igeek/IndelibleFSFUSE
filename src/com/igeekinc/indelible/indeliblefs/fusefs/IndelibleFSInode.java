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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.CreateDirectoryInfo;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileLike;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.luwak.FUSEAttr;
import com.igeekinc.luwak.inode.CreateInfo;
import com.igeekinc.luwak.inode.FUSEInode;
import com.igeekinc.luwak.inode.FUSEReqInfo;
import com.igeekinc.luwak.inode.exceptions.ExistsException;
import com.igeekinc.luwak.inode.exceptions.InodeException;
import com.igeekinc.luwak.inode.exceptions.InodeIOException;
import com.igeekinc.luwak.inode.exceptions.IsDirectoryException;
import com.igeekinc.luwak.inode.exceptions.NoEntryException;
import com.igeekinc.luwak.inode.exceptions.NotDirectoryException;
import com.igeekinc.luwak.inode.exceptions.PermissionException;
import com.igeekinc.luwak.util.FUSEFileHandle;
import com.igeekinc.util.ClientFileMetaData;
import com.igeekinc.util.ClientFileMetaDataProperties;
import com.igeekinc.util.logging.ErrorLogMessage;

public abstract class IndelibleFSInode extends FUSEInode<IndelibleFSFUSEVolume>
{
	IndelibleFSObjectID nodeID;
	WeakReference<IndelibleFileNodeIF> nodeRef;
	public IndelibleFSInode(IndelibleFSFUSEVolume indelibleFSVolume, IndelibleFileNodeIF node, long inodeNum, long generation, FUSEAttr attr) throws InodeIOException
	{
		super(indelibleFSVolume, inodeNum, generation, attr);
		setNode(node);
	}
	
	public IndelibleFileNodeIF getNode()
	{
		IndelibleFileNodeIF returnNode = nodeRef.get();
		if (returnNode == null)
		{
			try
			{
				returnNode = (IndelibleFileNodeIF)((IndelibleFSFUSEVolume)volume).indelibleFSVolume.getObjectByID(nodeID);
				setNode(returnNode);
			} catch (RemoteException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (ObjectNotFoundException e)
			{
				setNode(null);
			}
		}
		return returnNode;
	}
	
	public void setNode(IndelibleFileNodeIF node)
	{
		this.nodeRef = new WeakReference<IndelibleFileNodeIF>(node);
		nodeID = (IndelibleFSObjectID) node.getObjectID();
	}


	@Override
	public IndelibleFSInode lookup(FUSEReqInfo reqInfo, String name)
			throws InodeException, PermissionException
	{
		IndelibleFileNodeIF parentNode = getNode();
		IndelibleFSInode returnInode;
		try
		{
			if (parentNode.isDirectory())
			{
				IndelibleDirectoryNodeIF parentDir = (IndelibleDirectoryNodeIF)parentNode;
				try
				{
				IndelibleFileNodeIF child = parentDir.getChildNode(name);

					returnInode = volume.getInodeManager().retrieveInodeForID(child);
					if (returnInode == null)
					{
						synchronized(this)
						{
							int newInodeNum = volume.getNextInodeNum();
							returnInode = volume.createInode(child, newInodeNum, 0);
							volume.getInodeManager().addInode(returnInode);
						}
					}
				} catch (ObjectNotFoundException e)
				{
					throw new NoEntryException();
				}
				return returnInode;
			}
			else
			{
				throw new NotDirectoryException();
			}
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (PermissionDeniedException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new PermissionException();
		}

	}


	@Override
	public FUSEFileHandle open(FUSEReqInfo reqInfo, int flags)
			throws InodeException 
	{
		IndelibleFSFileHandle returnHandle = (IndelibleFSFileHandle) volume.getAdapter().allocateFileHandle();
		try
		{
			if (getNode().isDirectory() && (openForWrite(flags)))
				throw new IsDirectoryException();

			try
			{
				IndelibleFSForkIF dataFork = getNode().getFork("data", true);
				returnHandle.setInode(this);
				returnHandle.setOpenFork(dataFork);
			} catch (FileNotFoundException e)
			{
				throw new NoEntryException();
			} catch (ForkNotFoundException e)
			{
				throw new NoEntryException();
			} catch (PermissionDeniedException e)
			{
				throw new PermissionException();
			}
		} catch (IOException e1)
		{
			throw new InodeIOException();
		}
		if (openAndTruncate(flags))
			try
			{
				volume.getConnection(reqInfo).startTransaction();
				returnHandle.getOpenFork().truncate(0);
				volume.getConnection(reqInfo).commit();
			} catch (IOException e)
			{
				throw new PermissionException();
			}
		return returnHandle;
	}

	public IndelibleFSDirHandle opendir(FUSEReqInfo reqInfo, int flags) throws InodeException
	{
		IndelibleFSDirHandle returnHandle = (IndelibleFSDirHandle) volume.getAdapter().allocateDirHandle();
		
		IndelibleFileNodeIF node = getNode();
		try
		{
			if (node.isDirectory() && openForWrite(flags))
				throw new IsDirectoryException();	// Can't open for writing
			if (!node.isDirectory())
				throw new NotDirectoryException();
			returnHandle.setDir((IndelibleDirectoryNodeIF) node);
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		}
		return returnHandle;
	}
	
	public abstract boolean openForWrite(int flags);
	public abstract boolean openAndTruncate(int flags);
	
	@Override
	public CreateInfo<IndelibleFSFileHandle, IndelibleFSInode> create(FUSEReqInfo reqInfo, String name, int flags,
			int mode) throws InodeException
	{
		try
		{
			if (getNode().isDirectory())
			{
				synchronized(this)
				{
					volume.getConnection(reqInfo).startTransaction();
					IndelibleDirectoryNodeIF parentDir = (IndelibleDirectoryNodeIF)getNode();
					CreateFileInfo createInfo;
					try
					{
						// TODO - get the right value for exclusive from the flags or mode
						createInfo = parentDir.createChildFile(name, false);
					} catch (FileExistsException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						throw new ExistsException();
					}
					setNode(createInfo.getDirectoryNode());	// Indelible Directories are immutable, so we need to update to the new one
					int newInodeNum = volume.getNextInodeNum();

					ClientFileMetaData md = createRegularFileMD(reqInfo, mode);
					
					ClientFileMetaDataProperties mdProperties = md.getProperties();
					IndelibleFileNodeIF createFileNode = createInfo.getCreatedNode();
					createFileNode.setMetaDataResource(IndelibleFileLike.kClientFileMetaDataPropertyName, mdProperties.getMap());
					volume.getConnection(reqInfo).commit();
					IndelibleFSInode returnInode = volume.createInode(createInfo.getCreatedNode(), newInodeNum, 0);
					volume.getInodeManager().addInode(returnInode);
					IndelibleFSForkIF openFork;
					try
					{
						openFork = createFileNode.getFork("data", true);
					} catch (ForkNotFoundException e)
					{
						Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
						throw new InodeIOException();
					}
					if (openFork == null)
						throw new NoEntryException();
					IndelibleFSFileHandle returnHandle = (IndelibleFSFileHandle) volume.getAdapter().allocateFileHandle();
					returnHandle.setInode(returnInode);
					returnHandle.setOpenFork(openFork);
					
					CreateInfo<IndelibleFSFileHandle, IndelibleFSInode>returnInfo = new CreateInfo<IndelibleFSFileHandle, IndelibleFSInode>(returnHandle, returnInode);
					return returnInfo;
				}
			}
			else
			{
				throw new NotDirectoryException();
			}
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (PermissionDeniedException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new PermissionException();
		}
	}

	public abstract ClientFileMetaData createRegularFileMD(FUSEReqInfo reqInfo, int mode);
	
	@Override
	public IndelibleFSInode mkdir(FUSEReqInfo reqInfo, String name, int mode, int uMask)
			throws InodeException
	{
		IndelibleFSInode parent = volume.retrieveInode(reqInfo.getInodeNum());
		try
		{
			if (parent.getNode().isDirectory())
			{
				synchronized(parent)
				{
					volume.getConnection(reqInfo).startTransaction();
					boolean committed = false;
					try
					{
						IndelibleDirectoryNodeIF parentDir = (IndelibleDirectoryNodeIF)parent.getNode();
						CreateDirectoryInfo createInfo;
						try
						{
							createInfo = parentDir.createChildDirectory(name);
						} catch (FileExistsException e)
						{
							Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
							throw new ExistsException();
						}
						parent.setNode(createInfo.getDirectoryNode());
						int newInodeNum = volume.getNextInodeNum();

						ClientFileMetaData md = createDirectoryMD(reqInfo, mode);

						ClientFileMetaDataProperties mdProperties = md.getProperties();
						IndelibleDirectoryNodeIF createDirNode = createInfo.getCreatedNode();
						createDirNode.setMetaDataResource(IndelibleFileLike.kClientFileMetaDataPropertyName, mdProperties.getMap());
						volume.getConnection(reqInfo).commit();
						committed = true;
						IndelibleFSInode returnInode = volume.createInode(createInfo.getCreatedNode(), newInodeNum, 0);
						volume.getInodeManager().addInode(returnInode);
						return returnInode;
					}
					finally
					{
						if (!committed)
							volume.getConnection(reqInfo).rollback();
					}
				}
			}
			else
			{
				throw new NotDirectoryException();
			}
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (PermissionDeniedException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new PermissionException();
		}
	}

	public abstract ClientFileMetaData createDirectoryMD(FUSEReqInfo reqInfo, int mode);
	

	@Override
	public FUSEAttr getAttr(FUSEReqInfo reqInfo)
			throws InodeException
	{
		try
		{
			return attrForFile(getNode(), getInodeNum());
		} catch (PermissionDeniedException e)
		{
			throw new PermissionException();
		} catch (IOException e)
		{
			throw new InodeIOException();
		}
	}
	
	public abstract FUSEAttr attrForFile(IndelibleFileNodeIF attrFile, long inodeNum) throws InodeException, RemoteException, PermissionDeniedException, IOException;

}
