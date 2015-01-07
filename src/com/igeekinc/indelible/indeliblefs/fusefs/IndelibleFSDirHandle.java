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
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleDirectoryNodeRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFileNodeRemote;
import com.igeekinc.luwak.FUSEDirEntry;
import com.igeekinc.luwak.inode.DirectoryEntryBuffer;
import com.igeekinc.luwak.inode.FUSEReqInfo;
import com.igeekinc.luwak.inode.exceptions.InodeException;
import com.igeekinc.luwak.inode.exceptions.PermissionException;
import com.igeekinc.luwak.util.FUSEDirHandle;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleFSDirHandle extends FUSEDirHandle
{
	IndelibleDirectoryNodeRemote dir;
	ArrayList<FUSEDirEntry>dirEntries;
	
	protected IndelibleFSDirHandle(long handleNum)
	{
		super(handleNum);
	}

	
	static Charset utf8 = Charset.forName("UTF-8");
	
	public void setDir(IndelibleDirectoryNodeRemote dir) throws IOException, PermissionException
	{
		this.dir = dir;
		if (!dir.isDirectory())
			throw new IllegalArgumentException(dir.toString()+" is not a directory");

		reloadDirectoryEntries();
	}

	@Override
	public DirectoryEntryBuffer readdir(FUSEReqInfo reqInfo,
			long offset, int size, int flags,
			int readFlags) throws InodeException
	{
		DirectoryEntryBuffer returnBuffer = new DirectoryEntryBuffer(size);
		int index = getIndexForOffset(offset);
		int startIndex = index;
		if (index >= 0)
		{
			while(returnBuffer.getSpaceUsed() < returnBuffer.getMaxSize() && (index - startIndex < 15))
			{
				FUSEDirEntry curEntry = getEntry(index);
				if (curEntry == null)
					break;	// End of the line
				if (!returnBuffer.addDirEntry(curEntry))
					break;	// Out of space in the buffer
				index++;
			}
		}
		return returnBuffer;
	}
	
	public void reloadDirectoryEntries()
			throws IOException, RemoteException, PermissionException
	{
		String[] names;
		try
		{
			names = dir.list();
		} catch (PermissionDeniedException e)
		{
			throw new PermissionException();
		}
		dirEntries = new ArrayList<FUSEDirEntry>(names.length);
		long offset = 0;
		FUSEDirEntry dotEntry = new FUSEDirEntry(1, offset, FUSEDirEntry.DT_DIR, ".".getBytes(utf8));
		offset += dotEntry.getLength();
		dotEntry.setOffset(offset);
		dirEntries.add(dotEntry);
		
		FUSEDirEntry dotdotEntry = new FUSEDirEntry(1, offset, FUSEDirEntry.DT_DIR, "..".getBytes(utf8));
		offset += dotdotEntry.getLength();
		dotdotEntry.setOffset(offset);
		dirEntries.add(dotdotEntry);
		
		for (String curName:names)
		{
			try
			{
				IndelibleFileNodeRemote curFile = dir.getChildNode(curName);
				int type;
				if (curFile.isDirectory())
					type = FUSEDirEntry.DT_DIR;
				else
					if (curFile.isFile())
						type = FUSEDirEntry.DT_REG;
					else
						type = FUSEDirEntry.DT_UNKNOWN;	// Can we get here?
				byte [] nameBytes = curName.getBytes(utf8);
				FUSEDirEntry curEntry = new FUSEDirEntry(1, offset, type, nameBytes);	// inode is always 1 for now
				offset += curEntry.getLength();
				curEntry.setOffset(offset);
				dirEntries.add(curEntry);
			} catch (PermissionDeniedException e)
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			} catch (ObjectNotFoundException e)
			{
				// Must have been deleted - just skip
			}
		}
	}
	
	public IndelibleDirectoryNodeRemote getDir()
	{
		return dir;
	}
	public int getIndexForOffset(long offset)
	{
		if (offset == 0)
			return 0;
		int index = 0;
		for (FUSEDirEntry curEntry:dirEntries)
		{
			// The offsets in the dir entries are the offset of the NEXT entry.
			if (curEntry.getOffset() == offset)
				return index + 1;
			index++;
		}
		return -1;
	}
	
	public FUSEDirEntry getEntry(int index)
	{
		if (index >= dirEntries.size())
			return null;
		return dirEntries.get(index);
	}
	
	public void release()
	{

	}
}
