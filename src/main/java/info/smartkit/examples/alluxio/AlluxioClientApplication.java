package info.smartkit.examples.alluxio;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import java.io.IOException;

/**
 * Created by smartkit on 2016/12/22.
 * @see http://www.alluxio.org/docs/master/cn/File-System-API.html
 */
public class AlluxioClientApplication {

    public static void main(String[] args) throws IOException, AlluxioException {
        //CRUD file:
        createAlluxioFile();
        renameAlluxioFile();
        deleteAlluxioFile();


    }

    private static void renameAlluxioFile() throws IOException, AlluxioException {
        //Read file:
        FileSystem fileSystem = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI("/alluxioFile");
        //Open the file for reading and obtains a lock preventing delete.
        FileInStream in =  fileSystem.openFile(alluxioURI);
        // Rename
//        in.read();
        AlluxioURI alluxioURIR = new AlluxioURI("/alluxioFileR");
        if(!fileSystem.exists(alluxioURIR)) {
            fileSystem.rename(alluxioURI, alluxioURIR);
        }
        //Close file relinquishing the lock
//        in.close();
    }

    private static void createAlluxioFile() throws IOException, AlluxioException {
        FileSystem fileSystem = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI("/alluxioFile");
        //Generate operation to set a custom blocksize of 128MB
        CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(128* Constants.MB);
        if(!fileSystem.exists(alluxioURI)) {
            FileOutStream outStream = fileSystem.createFile(alluxioURI, options);
        }
    }
    private static void deleteAlluxioFile() throws IOException, AlluxioException {
        //Read file:
        FileSystem fileSystem = FileSystem.Factory.get();
        AlluxioURI alluxioURI = new AlluxioURI("/alluxioFile");
        //Open the file for reading and obtains a lock preventing delete.
//        FileSystem  in = fileSystem.openFile(alluxioURI);
        // Read data
        if(fileSystem.exists(alluxioURI)) {
            fileSystem.delete(alluxioURI);
            //Close file relinquishing the lock
            fileSystem.free(alluxioURI);
        }
    }
}
