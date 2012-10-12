#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <lwes.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define RECEIPT_TIME_OFFSET (2)
#define SENDER_IP_OFFSET    (RECEIPT_TIME_OFFSET+8)
#define SENDER_PORT_OFFSET  (SENDER_IP_OFFSET+4)
#define SITE_ID_OFFSET      (SENDER_PORT_OFFSET+2)
#define EXTENSION_OFFSET    (SITE_ID_OFFSET+2)
#define EVENT_TYPE_OFFSET   (EXTENSION_OFFSET+4)
#define HEADER_LENGTH       (EVENT_TYPE_OFFSET)              

struct ctxt {
  char key[100];
  char val[500];
};

static int struct_cmp_by_key (const void *a, const void *b)
{
  struct ctxt *ia = (struct ctxt *)a;
  struct ctxt *ib = (struct ctxt *)b;
  return strcmp (ia->key, ib->key);
}

static unsigned short header_uint16(const char* bytes) {
  return (unsigned short)ntohs(*((uint16_t*) bytes));
}

static unsigned int header_uint32(const char* bytes) {
  return (unsigned int)ntohl(*((unsigned int*) bytes));
}

static unsigned long long header_uint64(const char* bytes) {
  return (((unsigned long long)header_uint32(bytes))<<32) | header_uint32(bytes+4);
}

static unsigned short header_payload_length(const char* header)
{
  return (unsigned short)header_uint16(header);
}

static unsigned long long header_receipt_time(const char* header) {
  return (unsigned long long)header_uint64(header+RECEIPT_TIME_OFFSET);
}

int main(int argc, char **argv)
{
  gzFile file;
  const char *filename;
  const char *ctx_delim;
  char header[22];
  unsigned char buf[65535];
  struct lwes_event_deserialize_tmp *dtmp =
    (struct lwes_event_deserialize_tmp *)
      malloc (sizeof (struct lwes_event_deserialize_tmp));
/*  int badones = 0;*/
  if (argc == 2)
    {
      filename = argv[1];
      ctx_delim = "-";
    }
  else if (argc == 3)
    {
      filename = argv[1];
      ctx_delim = argv[2];
    }
  else
    {
      fprintf (stderr, "Usage: prog <journal> [<context delimiter> (defaults to '-') ]\n");
      exit (1);
    }
  file = gzopen (filename, "rb");
  while (gzread (file, header, 22) == 22) {
    unsigned short size = header_payload_length (header);
    unsigned long long timestamp =
      (unsigned long long)(header_receipt_time (header) / 1000);
    if (gzread (file, buf, size) != size)
    {
      fprintf (stderr, "error reading\n");
      exit (1);
    }

/*    char badfile[100];
    sprintf (badfile, "badone%05d",badones);
    FILE *bad = fopen (badfile,"wb");
    fwrite (header, sizeof(unsigned char), 22, bad);
    fwrite (buf, sizeof(unsigned char), size, bad);
    fclose(bad);
    badones++;
  */

    struct lwes_event *event = lwes_event_create_no_name ( NULL );
    int ret;
    ret = lwes_event_from_bytes (event, buf, size, 0, dtmp);
    LWES_SHORT_STRING event_name;
    ret = lwes_event_get_name (event, &event_name);
    if (strcmp (event_name, "MonDemand::StatsMsg") == 0)
      {
        char host_string[1000];
        LWES_LONG_STRING program;
        char key_buffer[31];
        LWES_U_INT_16 ctxt_num = 0;
        host_string[0] = '\0';

        struct ctxt ctxts[10]; 

        for (unsigned short i = 0; i < 10; i++)
          {
            ctxts[i].key[0] = '\0';
            ctxts[i].val[0] = '\0';
          }

        lwes_event_get_STRING (event, "prog_id", &program);

        lwes_event_get_U_INT_16 (event, "ctxt_num", &ctxt_num);
        LWES_U_INT_16 ctxts_len = 0;
        for (unsigned short i = 0 ; i < ctxt_num; i++)
          {
            LWES_LONG_STRING tmp_context_key;
            LWES_LONG_STRING tmp_context_value;
            snprintf(key_buffer, sizeof(key_buffer), "ctxt_k%d", i);
            lwes_event_get_STRING (event, key_buffer, &tmp_context_key);
            snprintf(key_buffer, sizeof(key_buffer), "ctxt_v%d", i);
            lwes_event_get_STRING (event, key_buffer, &tmp_context_value);
            if (strcmp (tmp_context_key, "host") == 0)
              {
                strcpy (host_string, tmp_context_value);
              }
            else
              {
                strcat (ctxts[ctxts_len].key, tmp_context_key);
                strcat (ctxts[ctxts_len].val, tmp_context_value);
                ctxts_len++;
              }
          }

        qsort (ctxts, ctxts_len, sizeof (struct ctxt), struct_cmp_by_key);

        char context_string[50000];
        context_string[0] = '\0';
        strcat (context_string, "-");
        for (unsigned short i = 0; i < ctxts_len; i++)
          {
            strcat (context_string, ctxts[i].key);
            strcat (context_string, "=");
            strcat (context_string, ctxts[i].val);
            strcat (context_string, ctx_delim);
          }
        size_t last = strlen (context_string);
        context_string[last-1] = '\0';

        LWES_U_INT_16 num = 0;
        lwes_event_get_U_INT_16 (event, "num", &num);
        for (LWES_U_INT_16 i = 0; i < num; i++)
          {
            LWES_LONG_STRING tmp_type = NULL;
            LWES_LONG_STRING tmp_key = NULL;
            LWES_INT_64 tmp_value = 0LL;
            snprintf(key_buffer, sizeof(key_buffer), "t%d", i);
            lwes_event_get_STRING (event, key_buffer, &tmp_type);
            snprintf(key_buffer, sizeof(key_buffer), "k%d", i);
            lwes_event_get_STRING (event, key_buffer, &tmp_key);
            snprintf(key_buffer, sizeof(key_buffer), "v%d", i);
            ret = lwes_event_get_INT_64 (event, key_buffer, &tmp_value);
            if (tmp_type == NULL)
              {
                /* backwards compatibility, send a gauge and a counter */
                fprintf (stdout, "gauge\t");
                fprintf (stdout, "%s\t", host_string);
                fprintf (stdout, "%s\t", program);
                fprintf (stdout, "%llu\t", timestamp);
                fprintf (stdout, "%s\t", tmp_key);
                fprintf (stdout, "%lld\t", tmp_value);
                fprintf (stdout, "%s\n", context_string);

                fprintf (stdout, "counter\t");
                fprintf (stdout, "%s\t", host_string);
                fprintf (stdout, "%s\t", program);
                fprintf (stdout, "%llu\t", timestamp);
                fprintf (stdout, "%s\t", tmp_key);
                fprintf (stdout, "%lld\t", tmp_value);
                fprintf (stdout, "%s\n", context_string);
              }
            else
              {
                fprintf (stdout, "%s\t", tmp_type);
                fprintf (stdout, "%s\t", host_string);
                fprintf (stdout, "%s\t", program);
                fprintf (stdout, "%llu\t", timestamp);
                fprintf (stdout, "%s\t", tmp_key);
                fprintf (stdout, "%lld\t", tmp_value);
                fprintf (stdout, "%s\n", context_string);
              }
          }
      }
    lwes_event_destroy (event);
  } 
  gzclose (file);
  free (dtmp);
  exit (0);
}
