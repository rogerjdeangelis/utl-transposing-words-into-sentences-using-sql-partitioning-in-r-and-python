%let pgm=utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python;

Transposing words into sentences using sql partitioning in r and python

I know, wps proc transpose, can easily solve this problem, but I want to show R and Python sql solutions.

github
https://tinyurl.com/444nvehx
https://github.com/rogerjdeangelis/utl-transposing-words-into-sentences-using-sql-partitioning-in-r-and-python

stackoverflow R
https://tinyurl.com/bdh5tah5
https://stackoverflow.com/questions/77571002/how-to-retrieve-all-texts-that-a-word-appear

None of the posted R solutions create a dataframe.

1. r hardcode sql
2. r dynamic sql
3. related repos
4. all r solution
5. same code will work in python pandasql (see repos on end)

SOAPBOX ON

  Although the R solutions are simple, none of th solutions result in a dataframe.
  I consider this a flaw in R solutions because converting datastructures to
  dataframes can be very challenging. SQL guanrantees a dataframe and
  the code is universal. Python is even more troublesome.

  Converting the R output list datastructures to a dataframe can take more code
  then some of the R solutions.

SOAPBOX OFF


github Macro to enable partitioning in wps proc sql.
https://tinyurl.com/3c3vzps5
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
 input word $1. text;
cards4;
a 1
a 2
a 5
b 1
b 3
c 1
c 3
c 4
d 4
e 2
e 4
f 3
g 2
h 5
i 5
;;;;
run;quit;

/*            _                   _               _          _             _
/ |    _ __  | |__   __ _ _ __ __| | ___ ___   __| | ___  __| |  ___  __ _| |
| |   | `__| | `_ \ / _` | `__/ _` |/ __/ _ \ / _` |/ _ \/ _` | / __|/ _` | |
| |_  | |    | | | | (_| | | | (_| | (_| (_) | (_| |  __/ (_| | \__ \ (_| | |
|_(_) |_|    |_| |_|\__,_|_|  \__,_|\___\___/ \__,_|\___|\__,_| |___/\__, |_|
                                                                        |_|
*/

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
   select
      word
     ,max(case when partition=1 then text else NULL end) as text1
     ,max(case when partition=2 then text else NULL end) as text2
     ,max(case when partition=3 then text else NULL end) as text3
   from
     ( select word, text, row_number() OVER (PARTITION BY word) as partition from have )
   group
     by word;
   ');
want;
endsubmit;
import data=sd1.want r=want;
run;quit;
");

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.WANT total obs=9                                                                                                   */
/*                                                                                                                        */
/*  WORD    TEXT1    TEXT2    TEXT3                                                                                       */
/*                                                                                                                        */
/*   a        1        2        5                                                                                         */
/*   b        1        3        .                                                                                         */
/*   c        1        3        4                                                                                         */
/*   d        4        .        .                                                                                         */
/*   e        2        4        .                                                                                         */
/*   f        3        .        .                                                                                         */
/*   g        2        .        .                                                                                         */
/*   h        5        .        .                                                                                         */
/*   i        5        .        .                                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/


/*___               _                             _                  _
|___ \   _ __    __| |_   _ _ __   __ _ _ __ ___ (_) ___   ___  __ _| |
  __) | | `__|  / _` | | | | `_ \ / _` | `_ ` _ \| |/ __| / __|/ _` | |
 / __/  | |    | (_| | |_| | | | | (_| | | | | | | | (__  \__ \ (_| | |
|_____| |_|     \__,_|\__, |_| |_|\__,_|_| |_| |_|_|\___| |___/\__, |_|
                      |___/                                       |_|
*/

%let maxWords=3;

%array(_wd,values=1-&maxWords);

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
libname sd1 'd:/sd1';
proc r;
export data=sd1.have r=have;
submit;
library(sqldf);
want<-sqldf('
   select
      word
     ,%do_over(_wd,phrase=%str(
         max(case when partition=? then text else NULL end) as text?),between=comma)
   from
     ( select word, text, row_number() OVER (PARTITION BY word) as partition from have )
   group
     by word;
   ');
want;
endsubmit;
import data=sd1.want r=want;
run;quit;
");

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SD1.WANT total obs=9                                                                                                   */
/*                                                                                                                        */
/*  WORD    TEXT1    TEXT2    TEXT3                                                                                       */
/*                                                                                                                        */
/*   a        1        2        5                                                                                         */
/*   b        1        3        .                                                                                         */
/*   c        1        3        4                                                                                         */
/*   d        4        .        .                                                                                         */
/*   e        2        4        .                                                                                         */
/*   f        3        .        .                                                                                         */
/*   g        2        .        .                                                                                         */
/*   h        5        .        .                                                                                         */
/*   i        5        .        .                                                                                         */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*
 _ __ ___ _ __   ___  ___
| `__/ _ \ `_ \ / _ \/ __|
| | |  __/ |_) | (_) \__ \
|_|  \___| .__/ \___/|___/
         |_|
*/

REPO
----------------------------------------------------------------------------------------------------------------------------------
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning


/*                _       _   _
 _ __   ___  ___ | |_   _| |_(_) ___  _ __  ___
| `__| / __|/ _ \| | | | | __| |/ _ \| `_ \/ __|
| |    \__ \ (_) | | |_| | |_| | (_) | | | \__ \
|_|    |___/\___/|_|\__,_|\__|_|\___/|_| |_|___/

*/


/*---- runs all the R solutions                                          ----*/


%utl_submit_wps64x('
libname sd1 "d:/sd1";
proc r;
export data=sd1.have r=have;
submit;
library(tidyverse);
want<-split(have$TEXT, have$WORD);
str(want);
txx<-unstack(have, TEXT~WORD);
str(txx);
xx<-map(unique(have$WORD),\(x){
        have %>% filter(WORD==x) %>% pull(TEXT)
  }) %>%
  purrr::set_names(unique(have$WORD));
str(xx);
endsubmit;
');

They all output lists

The WPS System

List of 9
 $ a: num [1:3] 1 2 5
 $ b: num [1:2] 1 3
 $ c: num [1:3] 1 3 4
 $ d: num 4
 $ e: num [1:2] 2 4
 $ f: num 3
 $ g: num 2
 $ h: num 5
 $ i: num 5

List of 9
 $ a: num [1:3] 1 2 5
 $ b: num [1:2] 1 3
 $ c: num [1:3] 1 3 4
 $ d: num 4
 $ e: num [1:2] 2 4
 $ f: num 3
 $ g: num 2
 $ h: num 5
 $ i: num 5

List of 9
 $ a: num [1:3] 1 2 5
 $ b: num [1:2] 1 3
 $ c: num [1:3] 1 3 4
 $ d: num 4
 $ e: num [1:2] 2 4
 $ f: num 3
 $ g: num 2
 $ h: num 5
 $ i: num 5


/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
