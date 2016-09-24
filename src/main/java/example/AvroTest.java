package example;

import com.entity.Book;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by vinod on 9/19/16.
 */
public class AvroTest {
    public static void main(final String[] args) throws IOException {
        Book b1 = Book.newBuilder().setName("First Book").setId(111).setCategory("Fiction").build();
        System.out
                .println("serializing books to temp file: ");
        DatumWriter<Book> userDatumWriter = new SpecificDatumWriter<Book>(Book.class);
        DataFileWriter<Book> dataFileWriter = new DataFileWriter<Book>(userDatumWriter);
        dataFileWriter.create(b1.getSchema(), new File("target/book.avro"));
        dataFileWriter.append(b1);
        dataFileWriter.close();
        DatumReader<Book> bookDatumReader = new SpecificDatumReader<Book>(
                Book.class);
        DataFileReader<Book> bookFileReader = new DataFileReader<Book>(new File("target/book.avro"),
                bookDatumReader);
        while (bookFileReader.hasNext()) {
            Book b = bookFileReader.next();
            System.out.println("deserialized from file: " + b);
        }
    }
}
