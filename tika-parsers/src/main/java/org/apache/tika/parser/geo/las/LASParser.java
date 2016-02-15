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
package org.apache.tika.parser.geo.las;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Tika parser for LAS ASCII files, standard CWLS.
 * 
 * @author gusai - FranceLabs on 15 Feb 2016
 */
public class LASParser extends AbstractParser {

	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = -6684471746645635590L;

	/**
	 * Supported file extensions *.las
	 */
	private static final MediaType LAS_EXT = MediaType.text("las");

	private static final String LAS_MIME = "text/las";
	private static final String LAS_DCMI_TYPE = "Dataset";
	private static final String LAS_CHARSET = "US-ASCII";

	private static final Set<MediaType> SUPPORTED_TYPES = Collections
			.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(LAS_EXT)));

	public Set<MediaType> getSupportedTypes(ParseContext context) {
		return SUPPORTED_TYPES;
	}

	public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
			throws IOException, SAXException, TikaException {

		/*
		 * Here we set only the MIME type, the DCMI type and the contents
		 * extracted from the header of the LAS files. All the other metadata
		 * are already extracted by other Tika parsers, MCF or will be extracted
		 * by Solr update processor.
		 */
		// Override the stream_content_type from application/octet_stream to
		// application/las
		metadata.set("stream_content_type", LAS_MIME);
		metadata.set(TikaCoreProperties.FORMAT, LAS_MIME);
		metadata.set(TikaCoreProperties.TYPE, LAS_DCMI_TYPE);
		// Content's language detection will be done by Solr update processor
		metadata.set("content", IOUtils.toString(stream, LAS_CHARSET));
		// File extension metadata extracted by Solr
		// ID metadata extracted by MCF
		// Title metadata extracted by Solr
		// URL metadata extracted by MCF
		// Language detection done by Solr update processor

	}
}
