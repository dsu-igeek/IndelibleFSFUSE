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
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.igeekinc.luwak.inode.exceptions.PermissionException;
import com.igeekinc.luwak.util.FUSEHandleManager;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSHandleManager extends FUSEHandleManager<IndelibleFSFileHandle, IndelibleFSDirHandle>
{

	@Override
	protected IndelibleFSDirHandle allocateDirHandle(long handleNum)
	{
		return new IndelibleFSDirHandle(handleNum);
	}

	@Override
	protected IndelibleFSFileHandle allocateFileHandle(long handleNum)
	{
		return new IndelibleFSFileHandle(handleNum);
	}

	/**
	 * Finds any dir handles that might reference this inode and reloads their entries
	 * @param inodeToReload
	 */
	public synchronized void reloadEntriesForDir(IndelibleFSInode inodeToReload)
	{
		for (IndelibleFSDirHandle curHandle:dirHandles.values())
		{
			if (curHandle.getDir().equals(inodeToReload.getNode()))
			{
				try
				{
					curHandle.reloadDirectoryEntries();
				} catch (RemoteException e)
				{
					// TODO Auto-generated catch block
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				} catch (PermissionException e)
				{
					// TODO Auto-generated catch block
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				}
			}
		}
	}
}
