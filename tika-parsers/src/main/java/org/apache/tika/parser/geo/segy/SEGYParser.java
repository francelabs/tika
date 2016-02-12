/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tika.parser.geo.segy;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import sigrun.common.ParseProgressListener;
import sigrun.common.SEGYStream;
import sigrun.common.SEGYStreamFactory;
import sigrun.common.SeismicTrace;
import sigrun.common.TextHeader;
import sigrun.serialization.BinaryHeaderFormat;
import sigrun.serialization.BinaryHeaderFormatBuilder;
import sigrun.serialization.FormatEntry;
import sigrun.serialization.TraceHeaderFormat;
import sigrun.serialization.TraceHeaderFormatBuilder;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Tika parser for SEG-Y files.
 * 
 * @author gusai - FranceLabs on 28 Jan 2016
 */
public class SEGYParser extends AbstractParser {

	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = -1972592936573598449L;

	/**
	 * Supported file extensions *.seg, *.segy, *.sgy
	 */
	private static final MediaType SEG_EXT = MediaType.application("seg");
	private static final MediaType SEGY_EXT = MediaType.application("segy");
	private static final MediaType SGY_EXT = MediaType.application("sgy");

	private static final String SEGY_MIME = "application/segy";
	private static final String SEGY_DCMI_TYPE = "Dataset";
	private static final String SEGY_CHARSET = "Cp1047";

	private static final Set<MediaType> SUPPORTED_TYPES = Collections
			.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(SEGY_EXT, SGY_EXT, SEG_EXT)));

	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;
	}

	public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
			throws IOException, SAXException, TikaException {

		/*
		 * Check if the mandatory charset is available. If not, throw exception.
		 */
		if (Charset.isSupported(SEGY_CHARSET)) {

			/*
			 * Create a SEGYStreamFactory with the specific headers format we
			 * want to use.
			 */
			SEGYStreamFactory streamFactory = SEGYStreamFactory.create(Charset.forName(SEGY_CHARSET),
					makeBinHeaderFormat(), makeTraceHeaderFormat());

			ReadableByteChannel channel = Channels.newChannel(stream);

			SEGYStream segyStream = streamFactory.makeStream(channel);

			StringBuilder strBuilder = new StringBuilder();
			//@formatter:off
			strBuilder
				.append(segyStream.getBinaryHeader().getDataSampleCode().toString())
				.append(" ")
				.append(extractTextHeader(segyStream.getTextHeader()))
				.append(" ");
			// We don't extract the traces information, only the header is useful
			//	.append(extractSeismicInfo(segyStream));
			//@formatter:on

			/*
			 * Here we set only the MIME type, the DCMI type and the contents
			 * extracted from the header of the SEG-Y files. All the other
			 * metadata are already extracted by other Tika parsers, MCF or will be extracted by Solr update processor.
			 */
			// Override the stream_content_type from application/octet_stream to application/segy
			metadata.set("stream_content_type", SEGY_MIME);
			metadata.set(TikaCoreProperties.FORMAT, SEGY_MIME);
			metadata.set(TikaCoreProperties.TYPE, SEGY_DCMI_TYPE);
			// Content's language detection will be done by Solr update processor
			metadata.set("content", strBuilder.toString());
			// File extension metadata extracted by Solr
			// ID metadata extracted by MCF
			// Title metadata extracted by Solr
		    // URL metadata extracted by MCF
		    // Language detection done by Solr update processor

		} else {
			throw new TikaException("org.apache.tika.parser.geo.segy.SEGYParser: charset " + SEGY_CHARSET
					+ " is not supported; the SEGY file cannot be parsed.");
		}
	}

	/**
	 * Creates a set of ParseProgressListener, with a dummy element whose
	 * progress method does nothing.
	 * 
	 * @return a set of ParseProgressListener with one dummy element.
	 */
	private static Set<ParseProgressListener> makeListenerSet() {

		Set<ParseProgressListener> result = new HashSet<ParseProgressListener>();

		result.add(new ParseProgressListener() {
			public void progress(long read) {

			}
		});

		return result;
	}

	/**
	 * Creates a SEG-Y binary header following a specific format.
	 * 
	 * @return an object representing the format of a SEG-Y binary header.
	 */
	private static BinaryHeaderFormat makeBinHeaderFormat() {
		return

		BinaryHeaderFormatBuilder.aBinaryHeaderFormat().withLineNumberFormat(FormatEntry.create(4, 8))
				.withSampleIntervalFormat(FormatEntry.create(16, 18))
				.withSamplesPerDataTraceFormat(FormatEntry.create(20, 22))
				.withDataSampleCodeFormat(FormatEntry.create(24, 26))
				.withSegyFormatRevNumberFormat(FormatEntry.create(300, 302))
				.withFixedLengthTraceFlagFormat(FormatEntry.create(302, 304))
				.withNumberOf3200ByteFormat(FormatEntry.create(304, 306)).build();
	}

	/**
	 * Creates a SEG-Y trace header following a specific format.
	 * 
	 * @return an object representing the format of a SEG-Y trace header.
	 */
	private static TraceHeaderFormat makeTraceHeaderFormat() {
		return

		TraceHeaderFormatBuilder.aTraceHeaderFormat().withEnsembleNumberFormat(FormatEntry.create(20, 24))
				.withSourceXFormat(FormatEntry.create(72, 76)).withSourceYFormat(FormatEntry.create(76, 80))
				.withXOfCDPPositionFormat(FormatEntry.create(180, 184))
				.withYOfCDPPositionFormat(FormatEntry.create(184, 188))
				.withNumberOfSamplesFormat(FormatEntry.create(114, 116)).build();
	}

	/**
	 * Extracts the header contents.
	 * 
	 * @param header
	 * @return a String containing the header contents.
	 */
	private static String extractTextHeader(TextHeader header) {

		StringBuilder strBuilder = new StringBuilder();
		for (String content : header.getContents()) {
			strBuilder.append(content);
		}
		return strBuilder.toString();
	}

	/**
	 * Extracts the seismic information from the traces in the stream in input.
	 * 
	 * @param header
	 * @return a String containing some valuable information on each trace.
	 */
	private static String extractSeismicInfo(SEGYStream segyStream) {
		StringBuilder strBuilder = new StringBuilder();
		for (SeismicTrace trace : segyStream) {
			strBuilder.append(stringifyTraceInfo(trace));
		}
		return strBuilder.toString();
	}

	/**
	 * Extract some valuable information from the trace in input.
	 * 
	 * @param trace
	 * @return Trace information, as a string: <br/>
	 *         - Number of samples in the trace, <br/>
	 *         - Size of the trace, <br/>
	 *         - The maximum and minimum values contained in the trace, <br/>
	 *         - The difference between the maximum and minimum values in the
	 *         trace. <br/>
	 */
	private static String stringifyTraceInfo(SeismicTrace trace) {
		//@formatter:off
		return new StringBuilder()
			.append("Trace Header info... ")
			.append(" Number of samples: ")
			.append(trace.getHeader().getNumberOfSamples())
			.append(" Size of array: ")
			.append(trace.getValues().length)
			.append(" Max Value: ")
			.append(trace.getMax())
			.append(" Min Value: ")
			.append(trace.getMin())
			.append(" Diff: ")
			.append(trace.getMax() - trace.getMin()).toString();
		//@formatter:on
	}
}
