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
import java.nio.ByteBuffer;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.luwak.inode.FUSEReqInfo;
import com.igeekinc.luwak.inode.exceptions.InodeException;
import com.igeekinc.luwak.inode.exceptions.InodeIOException;
import com.igeekinc.luwak.util.FUSEFileHandle;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSFileHandle extends FUSEFileHandle
{
	IndelibleFSForkIF openFork;
	IndelibleFSInode inode;
	
	protected IndelibleFSFileHandle(long handleNum)
	{
		super(handleNum);	
	}

	public void setOpenFork(IndelibleFSForkIF openFork)
	{
		this.openFork = openFork;
	}
	
	public IndelibleFSForkIF getOpenFork()
	{
		return openFork;
	}
	
	public void setInode(IndelibleFSInode createNode)
	{
		this.inode = createNode;
	}

	@Override
	public void release()
	{
		try
		{
			openFork.flush();
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		openFork = null;
	}

	@Override
	public int read(FUSEReqInfo reqInfo, long offset, byte[] returnBuffer, int flags, int readFlags)
			throws InodeException
	{
		IndelibleFSForkIF readFork = getOpenFork();
		try
		{
			CASIDDataDescriptor readDescriptor = readFork.getDataDescriptor(offset, returnBuffer.length);
			return readDescriptor.getData(returnBuffer, 0, 0, returnBuffer.length, true);
		} catch (RemoteException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			throw new InodeIOException();
		}
	}


	@Override
	public int write(FUSEReqInfo reqInfo, long offset, ByteBuffer writeBytes, int writeFlags) throws InodeException
	{
		IndelibleFSForkIF writeFork = getOpenFork();
		boolean transactionSuccessful = false;
		try
		{
			inode.getVolume().connection.startTransaction();
			CASIDMemoryDataDescriptor writeDataDescriptor = new CASIDMemoryDataDescriptor(writeBytes);
			writeFork.writeDataDescriptor(offset, writeDataDescriptor);
			inode.getVolume().connection.commit();
			transactionSuccessful = true;
			return (int) writeDataDescriptor.getLength();
		} catch (IOException e)
		{
			throw new InodeIOException();
		}
		finally
		{
			if (!transactionSuccessful)
				try
				{
					inode.getVolume().connection.rollback();
				} catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

	}
	
	@Override
	public void flush(FUSEReqInfo reqInfo, long lockOwnerFlags) throws InodeException
	{
			try
			{
				getOpenFork().flush();
			} catch (RemoteException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new InodeIOException();
			} catch (IOException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
				throw new InodeIOException();
			}
	}
	
	
}
