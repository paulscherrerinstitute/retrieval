## Current directory hierarchy

Description of the 1st version of the directory hierarchy used by `databuffer` since about year 2016:

```
${prefix}
  ${base_keyspace_name} (a.k.a. baseDir) e.g. "daq_swissfel"
    "config"
      ${channel_name}
        "latest"
          "00000_Config" ("00000" = version number serializer)
    ${base_keyspace_name}_{2,3,4}  (2=scalar, 3=waveform, 4=image)
      "byTime"
        ${channel_name}
          printf("%019d", $bin)  (e.g. "0000000000000017741") where $bin = $begin_ms / $bin_size_ms
            printf("%010d", $split)  (e.g. "0000000000") where $split starting at 0 - basically this is the machine/writer id/number
              printf("%019d", $bin_size_ms)_${serializer_version}_Data  (e.g. "0000000000086400000")  (currently always $serializer_version="00000"
              printf("%019d", $bin_size_ms)_${serializer_version}_Data_Index
              printf("%019d", $bin_size_ms)_${serializer_version}_Stats              ??? used ?
              printf("%019d", $bin_size_ms)_Data_TTL                                 ??? used ?
              printf("%019d", $bin_size_ms)_Stats_TTL                                ??? used ?
    "meta"
      "byPulse"  ???
      "byTime"   ???
```

So far the bin size for images is 3600000, all others seem to use 86400000.
