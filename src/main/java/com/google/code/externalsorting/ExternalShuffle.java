package com.google.code.externalsorting;

// filename: ExternalShuffle.java

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Goal: offer a generic external-memory shuffling program in Java.
 *
 * It must be : - hackable (easy to adapt) - scalable to large files - sensibly
 * efficient.
 *
 * This software is in the public domain.
 *
 * Usage: java com/google/code/externalsorting/ExternalShuffle somefile.txt out.txt
 *
 * You can change the default maximal number of temporary files with the -t
 * flag: java com/google/code/externalsorting/ExternalShuffle somefile.txt out.txt
 * -t 3
 *
 * For very large files, you might want to use an appropriate flag to allocate
 * more memory to the Java VM: java -Xms2G
 * com/google/code/externalsorting/ExternalShuffle somefile.txt out.txt
 *
 * By (in alphabetical order) Philippe Beaudoin, Eleftherios Chetzakis, Jon
 * Elsas, Christan Grant, Daniel Haran, Daniel Lemire, Sugumaran Harikrishnan,
 * Amit Jain, Thomas Mueller, Jerry Yang, First published: April 2010 originally posted at
 * http://lemire.me/blog/archives/2010/04/01/external-memory-shuffling-in-java/
 */
public class ExternalShuffle {


        private static void displayUsage() {
                System.out
                        .println("java com.google.externalsorting.ExternalShuffle inputfile outputfile");
                System.out.println("Flags are:");
                System.out.println("-v or --verbose: verbose output");
                System.out
                        .println("-t or --maxtmpfiles (followed by an integer): specify an upper bound on the number of temporary files");
                System.out
                        .println("-c or --charset (followed by a charset code): specify the character set to use (for shuffling)");
                System.out
                        .println("-z or --gzip: use compression for the temporary files");
                System.out
                        .println("-H or --header (followed by an integer): ignore the first few lines");
                System.out
                        .println("-s or --store (following by a path): where to store the temporary files");
                System.out.println("-h or --help: display this message");
        }

        /**
         * This method calls the garbage collector and then returns the free
         * memory. This avoids problems with applications where the GC hasn't
         * reclaimed memory and reports no available memory.
         *
         * @return available memory
         */
        public static long estimateAvailableMemory() {
          System.gc();
          // http://stackoverflow.com/questions/12807797/java-get-available-memory
          Runtime r = Runtime.getRuntime();
          long allocatedMemory = r.totalMemory() - r.freeMemory();
          long presFreeMemory = r.maxMemory() - allocatedMemory;
          return presFreeMemory;
        }

        /**
         * we divide the file into small blocks. If the blocks are too small, we
         * shall create too many temporary files. If they are too big, we shall
         * be using too much memory.
         *
         * @param sizeoffile how much data (in bytes) can we expect
         * @param maxtmpfiles how many temporary files can we create (e.g., 1024)
         * @param maxMemory Maximum memory to use (in bytes)
         * @return the estimate
         */
        public static long estimateBestSizeOfBlocks(final long sizeoffile,
                final int maxtmpfiles, final long maxMemory) {
                // we don't want to open up much more than maxtmpfiles temporary
                // files, better run
                // out of memory first.
                long blocksize = sizeoffile / maxtmpfiles
                        + (sizeoffile % maxtmpfiles == 0 ? 0 : 1);

                // on the other hand, we don't want to create many temporary
                // files
                // for naught. If blocksize is smaller than half the free
                // memory, grow it.
                if (blocksize < maxMemory / 2) {
                        blocksize = maxMemory / 2;
                }
                return blocksize;
        }

        /**
         * @param args command line argument
         * @throws IOException generic IO exception
         */
        public static void main(final String[] args) throws IOException {
                boolean verbose = false;
                int maxtmpfiles = DEFAULTMAXTEMPFILES;
                Charset cs = Charset.defaultCharset();
                String inputfile = null, outputfile = null;
                File tempFileStore = null;
                boolean usegzip = false;
                int headersize = 0;
                for (int param = 0; param < args.length; ++param) {
                        if (args[param].equals("-v")
                                || args[param].equals("--verbose")) {
                                verbose = true;
                        } else if ((args[param].equals("-h") || args[param]
                                .equals("--help"))) {
                                displayUsage();
                                return;
                        } else if ((args[param].equals("-t") || args[param]
                                .equals("--maxtmpfiles"))
                                && args.length > param + 1) {
                                param++;
                                maxtmpfiles = Integer.parseInt(args[param]);
                                if (maxtmpfiles < 0) {
                                        System.err
                                                .println("maxtmpfiles should be positive");
                                }
                        } else if ((args[param].equals("-c") || args[param]
                                .equals("--charset"))
                                && args.length > param + 1) {
                                param++;
                                cs = Charset.forName(args[param]);
                        } else if ((args[param].equals("-z") || args[param]
                                .equals("--gzip"))) {
                                usegzip = true;
                        } else if ((args[param].equals("-H") || args[param]
                                .equals("--header")) && args.length > param + 1) {
                                param++;
                                headersize = Integer.parseInt(args[param]);
                                if (headersize < 0) {
                                        System.err
                                                .println("headersize should be positive");
                                }
                        } else if ((args[param].equals("-s") || args[param]
                                .equals("--store")) && args.length > param + 1) {
                                param++;
                                tempFileStore = new File(args[param]);
                        } else {
                                if (inputfile == null) {
                                        inputfile = args[param];
                                } else if (outputfile == null) {
                                        outputfile = args[param];
                                } else {
                                        System.out.println("Unparsed: "
                                                + args[param]);
                                }
                        }
                }
                if (outputfile == null) {
                        System.out
                                .println("please provide input and output file names");
                        displayUsage();
                        return;
                }
                ThreadLocalRandom random = ThreadLocalRandom.current();
                List<LineCountFile> l = shuffInBatch(new File(inputfile), random,
                        maxtmpfiles, cs, tempFileStore, headersize,
                        usegzip);
                if (verbose) {
                        System.out
                                .println("created " + l.size() + " tmp files");
                }
                mergeShuffledFiles(l, new File(outputfile), random, cs, false, usegzip);
        }

        /**
         * This merges several BinaryFileBuffer to an output writer.
         *
         * @param fbw     A buffer where we write the data.
         * @param random  A random number generator.
         * @param buffers
         *                Where the data should be read.
         * @return The number of lines shuffled.
         * @throws IOException generic IO exception
         *
         */
        public static long mergeShuffledFiles(BufferedWriter fbw,
                final Random random, List<IOStringStack> buffers) throws IOException {

                int sum = 0;
                for(IOStringStack ss : buffers) sum += ss.size();
                long rowcounter = 0;
                while(sum > 0) {
                        int y = random.nextInt(sum); // random integer in [0,sum)
                        // select the key in linear time
                        int runningsum = 0;
                        int z = 0;
                        for(; z < buffers.size(); z++) {
                                runningsum += buffers.get(z).size();
                                if(y < runningsum) { break; }
                        }
                        IOStringStack bfb = buffers.get(z);
                        String r = bfb.pop();
                        fbw.write(r);
                        fbw.newLine();
                        ++rowcounter;
                        if (bfb.empty()) {
                                bfb.close();
                                buffers.remove(bfb);
                        }
                        sum -= 1;
                }
                fbw.close();
                return rowcounter;
        }


        /**
         * This merges a bunch of temporary flat files
         *
         * @param files The {@link List} of shuffled {@link File}s to be merged.
         * @param outputfile The output {@link File} to merge the results to.
         * @return The number of lines shuffled.
         * @throws IOException generic IO exception
         */
        public static long mergeShuffledFiles(List<LineCountFile> files, File outputfile)
                throws IOException {
                return mergeShuffledFiles(files, outputfile, ThreadLocalRandom.current(),
                        Charset.defaultCharset());
        }

        /**
         * This merges a bunch of temporary flat files
         *
         * @param files The {@link List} of shuffled {@link File}s to be merged.
         * @param outputfile The output {@link File} to merge the results to.
         * @param random The {@link Comparator} to use to compare
         *                {@link String}s.
         * @return The number of lines shuffled.
         * @throws IOException generic IO exception
         */
        public static long mergeShuffledFiles(List<LineCountFile> files, File outputfile,
                final Random random) throws IOException {
                return mergeShuffledFiles(files, outputfile, random,
                        Charset.defaultCharset());
        }

        /**
         * This merges a bunch of temporary flat files
         *
         * @param files The {@link List} of shuffled {@link File}s to be merged.
         * @param outputfile The output {@link File} to merge the results to.
         * @param random The {@link Comparator} to use to compare
         *                {@link String}s.
         * @param cs The {@link Charset} to be used for the byte to
         *                character conversion.
         * @return The number of lines shuffled.
         * @throws IOException generic IO exception
         * @since v0.1.2
         */
        public static long mergeShuffledFiles(List<LineCountFile> files, File outputfile,
                final Random random, Charset cs)
                throws IOException {
                return mergeShuffledFiles(files, outputfile, random, cs,
                        false, false);
        }

        /**
         * This merges a bunch of temporary flat files
         *
         * @param files The {@link List} of shuffled {@link File}s to be merged.
         * @param outputfile The output {@link File} to merge the results to.
         * @param random The {@link Comparator} to use to compare
         *                {@link String}s.
         * @param cs The {@link Charset} to be used for the byte to
         *                character conversion.
         * @param append Pass <code>true</code> if result should append to
         *                {@link File} instead of overwrite. Default to be false
         *                for overloading methods.
         * @param usegzip assumes we used gzip compression for temporary files
         * @return The number of lines shuffled.
         * @throws IOException generic IO exception
         * @since v0.1.4
         */
        public static long mergeShuffledFiles(List<LineCountFile> files, File outputfile,
                final Random random, Charset cs,
                boolean append, boolean usegzip) throws IOException {
                ArrayList<IOStringStack> bfbs = new ArrayList<>();
                for (LineCountFile f : files) {
                        final int BUFFERSIZE = 2048;
                        InputStream in = new FileInputStream(f.getFile());
                        BufferedReader br;
                        if (usegzip) {
                                br = new BufferedReader(
                                        new InputStreamReader(
                                                new GZIPInputStream(in,
                                                        BUFFERSIZE), cs));
                        } else {
                                br = new BufferedReader(new InputStreamReader(
                                        in, cs));
                        }

                        BinaryFileBuffer bfb = new BinaryFileBuffer(br, f.getLineCount());
                        bfbs.add(bfb);
                }
                BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(outputfile, append), cs));
                long rowcounter = mergeShuffledFiles(fbw, random, bfbs);
                for (LineCountFile f : files) {
                        f.getFile().delete();
                }
                return rowcounter;
        }

        /**
         * This shuffles a file (input) to an output file (output) using default
         * parameters
         *
         * @param input source file
         *
         * @param output output file
         * @throws IOException generic IO exception
         */
        public static void shuffle(final File input, final File output)
                throws IOException {
                ExternalShuffle.mergeShuffledFiles(ExternalShuffle.shuffInBatch(input),
                        output);
        }

        /**
         * This shuffles a file (input) to an output file (output) using customized comparator
         *
         * @param input source file
         *
         * @param output output file
         *
         * @param random The {@link Comparator} to use to compare
         *                {@link String}s.
         * @throws IOException generic IO exception
         */
        public static void shuffle(final File input, final File output, final Random random)
                throws IOException {
                ExternalShuffle.mergeShuffledFiles(ExternalShuffle.shuffInBatch(input, random),
                        output, random);
        }

        /**
         * shuffle a list and save it to a temporary file
         *
         * @return the file containing the shuffled data
         * @param tmplist data to be shuffled
         * @param random random number generator
         * @param cs charset to use for output (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param usegzip set to <code>true</code> if you are using gzip compression for the
         *                temporary files
         * @throws IOException generic IO exception
         */
        public static LineCountFile shuffleAndSave(List<String> tmplist,
                Random random, Charset cs, File tmpdirectory, boolean usegzip) throws IOException {
                Collections.shuffle(tmplist, random);
                File newtmpfile = File.createTempFile("shuffInBatch", "flatfile", tmpdirectory);
                newtmpfile.deleteOnExit();
                OutputStream out = new FileOutputStream(newtmpfile);
                int ZIPBUFFERSIZE = 2048;
                if (usegzip) {
                        out = new GZIPOutputStream(out, ZIPBUFFERSIZE) {
                                {
                                        this.def.setLevel(Deflater.BEST_SPEED);
                                }
                        };
                }
                try (BufferedWriter fbw = new BufferedWriter(new OutputStreamWriter(
                        out, cs))) {
                        for (String r : tmplist) {
                                fbw.write(r);
                                fbw.newLine();
                        }
                }
                return new LineCountFile(newtmpfile, tmplist.size());
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later.
         *
         * @param fbr data source
         * @param datalength estimated data volume (in bytes)
         * @param random random number generator
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(final BufferedReader fbr,
                final long datalength, final Random random) throws IOException {
                return shuffInBatch(fbr, datalength, random, DEFAULTMAXTEMPFILES,
                        estimateAvailableMemory(), Charset.defaultCharset(),
                        null, 0, false);
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later.
         *
         * @param fbr data source
         * @param datalength estimated data volume (in bytes)
         * @param random random number generator
         * @param maxtmpfiles maximal number of temporary files
         * @param maxMemory maximum amount of memory to use (in bytes)
         * @param cs character set to use (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param numHeader number of lines to preclude before shuffling starts
         * @param usegzip use gzip compression for the temporary files
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(final BufferedReader fbr,
                final long datalength, final Random random,
                final int maxtmpfiles, long maxMemory, final Charset cs,
                final File tmpdirectory,
                final int numHeader, final boolean usegzip)
                    throws IOException {
                List<LineCountFile> files = new ArrayList<>();
                long blocksize = estimateBestSizeOfBlocks(datalength,
                        maxtmpfiles, maxMemory);// in
                // bytes

                try {
                        List<String> tmplist = new ArrayList<>();
                        String line = "";
                        try {
                                int counter = 0;
                                while (line != null) {
                                        long currentblocksize = 0;// in bytes
                                        while ((currentblocksize < blocksize)
                                                && ((line = fbr.readLine()) != null)) {
                                                // as long as you have enough
                                                // memory
                                                if (counter < numHeader) {
                                                        counter++;
                                                        continue;
                                                }
                                                tmplist.add(line);
                                                currentblocksize += StringSizeEstimator
                                                        .estimatedSizeOf(line);
                                        }
                                        files.add(shuffleAndSave(tmplist, random, cs,
                                                tmpdirectory, usegzip));
                                        tmplist.clear();
                                }
                        } catch (EOFException oef) {
                                if (tmplist.size() > 0) {
                                        files.add(shuffleAndSave(tmplist, random, cs,
                                                tmpdirectory, usegzip));
                                        tmplist.clear();
                                }
                        }
                } finally {
                        fbr.close();
                }
                return files;
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later.
         *
         * @param file some flat file
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file) throws IOException {
                return shuffInBatch(file, ThreadLocalRandom.current());
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later.
         *
         * @param file some flat file
         * @param random random number generator
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random) throws IOException {
                return shuffInBatch(file, random, DEFAULTMAXTEMPFILES,
                        Charset.defaultCharset(), null);
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later. You can specify a bound on the number of temporary
         * files that will be created.
         *
         * @param file some flat file
         * @param random random number generator
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param numHeader number of lines to preclude before shuffling starts
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random,
                File tmpdirectory, int numHeader)
                throws IOException {
                return shuffInBatch(file, random, DEFAULTMAXTEMPFILES,
                        Charset.defaultCharset(), tmpdirectory,
                        numHeader);
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later. You can specify a bound on the number of temporary
         * files that will be created.
         *
         * @param file some flat file
         * @param random random number generator
         * @param maxtmpfiles maximal number of temporary files
         * @param cs  character set to use (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random,
                int maxtmpfiles, Charset cs, File tmpdirectory)
                throws IOException {
                return shuffInBatch(file, random, maxtmpfiles, cs, tmpdirectory, 0);
        }

                 /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later. You can specify a bound on the number of temporary
         * files that will be created.
         *
         * @param file some flat file
         * @param random random number generator
         * @param cs character set to use (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param numHeader number of lines to preclude before shuffling starts
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random,
                Charset cs, File tmpdirectory, int numHeader)
                throws IOException {
                BufferedReader fbr = new BufferedReader(new InputStreamReader(
                        new FileInputStream(file), cs));
                return shuffInBatch(fbr, file.length(), random, DEFAULTMAXTEMPFILES,
                        estimateAvailableMemory(), cs, tmpdirectory,
                        numHeader, false);
        }

         /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later. You can specify a bound on the number of temporary
         * files that will be created.
         *
         * @param file some flat file
         * @param random random number generator
         * @param maxtmpfiles maximal number of temporary files
         * @param cs character set to use (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param numHeader number of lines to preclude before shuffling starts
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random,
                int maxtmpfiles, Charset cs, File tmpdirectory, int numHeader)
                throws IOException {
                BufferedReader fbr = new BufferedReader(new InputStreamReader(
                        new FileInputStream(file), cs));
                return shuffInBatch(fbr, file.length(), random, maxtmpfiles,
                        estimateAvailableMemory(), cs, tmpdirectory,
                        numHeader, false);
        }

        /**
         * This will simply load the file by blocks of lines, then shuffle them
         * in-memory, and write the result to temporary files that have to be
         * merged later. You can specify a bound on the number of temporary
         * files that will be created.
         *
         * @param file some flat file
         * @param random random number generator
         * @param maxtmpfiles maximal number of temporary files
         * @param cs character set to use (can use
         *                Charset.defaultCharset())
         * @param tmpdirectory location of the temporary files (set to null for
         *                default location)
         * @param numHeader number of lines to preclude before shuffling starts
         * @param usegzip use gzip compression for the temporary files
         * @return a list of temporary flat files
         * @throws IOException generic IO exception
         */
        public static List<LineCountFile> shuffInBatch(File file, Random random,
                int maxtmpfiles, Charset cs, File tmpdirectory, int numHeader, boolean usegzip)
                throws IOException {
                BufferedReader fbr = new BufferedReader(new InputStreamReader(
                        new FileInputStream(file), cs));
                return shuffInBatch(fbr, file.length(), random, maxtmpfiles,
                        estimateAvailableMemory(), cs, tmpdirectory,
                        numHeader, usegzip);
        }

        /**
         * Default maximal number of temporary files allowed.
         */
        public static final int DEFAULTMAXTEMPFILES = 1024;
}
