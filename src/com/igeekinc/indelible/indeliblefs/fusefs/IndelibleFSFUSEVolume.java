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

import java.io.IOException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.luwak.FUSEStatFS;
import com.igeekinc.luwak.inode.FUSEInodeAdapter;
import com.igeekinc.luwak.inode.FUSEReqInfo;
import com.igeekinc.luwak.inode.FUSEVolumeBase;
import com.igeekinc.luwak.inode.exceptions.InodeException;
import com.igeekinc.util.logging.ErrorLogMessage;

public abstract class IndelibleFSFUSEVolume extends FUSEVolumeBase<IndelibleFSInode, IndelibleFSFileHandle, IndelibleFSDirHandle, IndelibleFSInodeManager, IndelibleFSHandleManager>
{
	IndelibleServerConnectionIF connection;
	IndelibleFSVolumeIF indelibleFSVolume;
	IndelibleFSInode root;
	Logger logger;
	int inodeNum;
	
	public IndelibleFSFUSEVolume(IndelibleServerConnectionIF connection, IndelibleFSVolumeIF indelibleFSVolume)
	{
		logger = Logger.getLogger(getClass());
		this.connection = connection;
		this.indelibleFSVolume = indelibleFSVolume;
		try
		{
			IndelibleDirectoryNodeIF rootNode = indelibleFSVolume.getRoot();
			inodeNum = 1;
			
			root = createInode(rootNode, inodeNum, 0);
			getInodeManager().addInode(getRoot());
			inodeNum++;
		} catch (IOException e)
		{
			logger.fatal("Got remote exception trying to retrieve root", e);
			throw new InternalError("Could not get root");
		} catch (PermissionDeniedException e)
		{
			logger.fatal("Got PermissionDeniedException trying to retrieve root", e);
			throw new InternalError("Could not get root");
		} catch (InodeException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InternalError("Got an I/O error trying to read root");
		}
		

	}
	
	public abstract IndelibleFSInode createInode(IndelibleFileNodeIF node, int inodeNum, int generation) throws InodeException;
	
	@Override
	public IndelibleFSHandleManager allocateHandleManager()
	{
		return new IndelibleFSHandleManager();
	}

	@Override
	public IndelibleFSInodeManager allocateInodeManager()
	{
		IndelibleFSInodeManager returnManager = new IndelibleFSInodeManager(this);
		return returnManager;
	}

	@Override
	public IndelibleFSInode getRoot()
	{
		return root;
	}

	@Override
	public IndelibleFSInode retrieveInode(long inodeNum)
	{
		return getInodeManager().retrieveInode(inodeNum);
	}

	@Override
	public FUSEInodeAdapter<IndelibleFSInode, IndelibleFSInodeManager, IndelibleFSFileHandle, IndelibleFSDirHandle, IndelibleFSHandleManager> getAdapter() 
	{
		return adapter;
	}
	
	@Override
	public FUSEStatFS getStatFS(FUSEReqInfo reqInfo)
			throws InodeException
	{
	    FUSEStatFS returnStatFS = new FUSEStatFS();

	    returnStatFS.setBAvail(1024*1024);
	    returnStatFS.setBlocks(1024*1024);
	    returnStatFS.setBSize(512);
	    returnStatFS.setFFree(1024*1024);
	    returnStatFS.setFiles(1);
	    returnStatFS.setFRSize(1);
	    returnStatFS.setNameLen(256);
	    return returnStatFS;
	}

	public int getNextInodeNum()
	{
		return inodeNum++;
	}
	

	public IndelibleServerConnectionIF getConnection(FUSEReqInfo reqInfo)
	{
		return connection;
	}
}
