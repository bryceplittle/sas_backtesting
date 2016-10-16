options mstored sasmstore = macstore;
libname macstore '\\MACRO_FOLDER';

/*** macros ***/

/* proc_expand: wraps SAS proc expand in a macro */
%macro proc_expand(data_set, oper, p, time_var, id, var_name, new_var) / store source;
	proc sort
		data	= &data_set;
		by		&id &time_var;
	run;

	/* basic expand procedure */
	proc expand
		data 	= &data_set
		out		= &data_set;
		convert
				&var_name = &new_var / transformout = (&oper &p);
		by
				&id;
	run;

	/* re-label new variable */
	data &data_set;
		set &data_set(drop = time);
			label &new_var = &new_lab;
	run;

	proc datasets library=work nolist;
  		modify &data_set;
  		attrib _all_ label='';
	quit;

run;
%mend;

/* proc_rank: wraps SAS proc rank in a macro */
%macro proc_rank(data_set, p, time_var, id, var_name) / store source;
	proc sort
		data	= &data_set;
		by		&time_var &id;
	run;

	proc rank
		data 	= &data_set
		out		= &data_set
		groups	= &p;
		by		&time_var;
		var		&var_name;
		ranks	&var_name._rank;
	run;

	data &data_set;
		set &data_set;
			if MISSING(&var_name._rank) = 0 then
				&var_name._rank = &var_name._rank + 1;
	run;
%mend;

/* form_port: forms portfolios based on a signal */
/*
	lag_months:	{positive integer}
	time_var:	{date,year}
*/
%macro form_port(data_set, signal_var, long_p, short_p, time_var, id, lag_months, price_data) / store source;

	/* create long and short portfolios */

	data &signal_var._long(keep = &time_var &id &signal_var);
		retain
			&id
			&time_var
			&signal_var;
		set &data_set;
			if &signal_var._rank = &long_p;
	run;
	
	data &signal_var._short(keep = &time_var &id &signal_var);
		retain
			&id
			&time_var
			&signal_var;
		set &data_set;
			if &signal_var._rank = &short_p;
	run;

	proc sort
		data	= &signal_var._long;
		by		&time_var &id;
	run;

	proc sort
		data	= &signal_var._short;
		by		&time_var &id;
	run;

	/* annual rebalancing */

	%if &time_var = year %then %do;

		data &signal_var._long;
			set &signal_var._long;
				&time_var = &time_var + 1;  /* iterate fiscal year forward one to prevent look-ahead */
		run;

		data &signal_var._short;
			set &signal_var._short;
				&time_var = &time_var + 1;
		run;

		data merge_dates;
			set crsp_dates;
				if month = &lag_months - 1;  /* month means month-end, so subtract one */
		run;

		data &signal_var._long;
			retain
				&id
				date
				year
				month
				&signal_var;
			merge &signal_var._long(in=k) merge_dates;
				by year;
				if k;
				if missing(date) then delete;
		run;

		data &signal_var._short;
			retain
				&id
				date
				year
				month
				&signal_var;
			merge &signal_var._short(in=k) merge_dates;
				by year;
				if k;
				if missing(date) then delete;
		run;
	%end;

	/* date-rebalancing */

	%if &time_var = date %then %do;
		/* TBD */
	%end;

	proc datasets library=work nolist;
  		modify &signal_var._long;
  		attrib _all_ label='';
	quit;

	proc datasets library=work nolist;
  		modify &signal_var._short;
  		attrib _all_ label='';
	quit;

	proc datasets 
		library		= work;
   		delete 		merge_dates;
	run;

%mend;


/* crsp_dates: gives list of unique month-end dates with price data*/
%macro crsp_dates(ret_data) / store source;
	data crsp_dates;
		set &ret_data(keep = date year month);
	run;

	proc sort
		data = crsp_dates;
		by date;
	run;

	data crsp_dates;
		set crsp_dates;
			by date;
			if first.date;
	run;

	proc sort
		data = crsp_dates;
		by date;
	run;
%mend;


/** fill_holdings: creates a monthly time-series of holdings from a less-frequent set of holdings **/

%macro fill_holdings(port_data, port_name, price_data, id) / store source;

	data rebal_dates(keep = date month year order);
		set &port_data;
			retain order 0;
			by date;
			if first.date;
			order = order + 1;
	run;

	proc sql noprint;
		select count(*)
			into :N
		from rebal_dates;
	quit;

	%do i = 1 %to &N;
	
		/* form a date window for a portfolio */	

		data _null_;
			set rebal_dates;
				if order = &i;
				call symput('redate', date);  /* macro variable for rebalance date */
		run;

		data _null_;
			set rebal_dates;
				if order = %eval(&i+1);
				call symput('fredate', date);  /* macro variable for next rebalance date */
		run;

		%if &i < &N %then %do;  /* create a bounded window if not last rebalance */
			data date_window;
				set crsp_dates;
					retain order 0;
					by date;
					if (date >= &redate) & (date < &fredate);
					order = order + 1;
			run;
		%end;
		%else %do;  /* use all dates thereafter for last rebalance */
			data date_window;
				set crsp_dates;
					retain order 0;
					by date;
					if (date >= &redate);
					order = order + 1;
			run;			
		%end;

		/* propagate holdings over window */

		proc sql noprint;
			select count(*)
				into :N_fill  /* count obs in date window */
			from date_window;
		quit;

		%put &N_fill;

		data temp_hold; /* stores holdings in memory */
			set &port_data;
				 if date = &redate;
		run;

		/* copy holdings over date window */
		%do j = 1 %to &N_fill;

			data _null_; /* read date &j of rebalance window into macro variable */
				set date_window;
					if order = &j;
					call symput('wredate', date);
			run;

			%if &j = 1 %then %do; /* first rebalance date is good for first copy */
				data holdings;
					set temp_hold;
				run;
			%end;
			%else %do; /* change the date for future holdings */
				data append_holdings;
					set temp_hold;
						date 	= &wredate;
				run;

				proc append  /* appends holdings with new date to holdings on rebalance date */
					base	= holdings
					data	= append_holdings;
				run;

			%end;
		%end;

		%if &i = 1 %then %do;  /* on first loop, main_holdings is just holdings */
			data &port_name._holdings;
				set holdings;
			run;
		%end;
		%else %do;
			proc append
				base	= &port_name._holdings
				data	= holdings;
			run;
		%end;
	%end;
	
	/* weight by market cap and equal weight */

	proc sort
		data 	= &port_name._holdings;
		by		date &id;
	run;

	proc sort
		data 	= &price_data;
		by		date &id;
	run;

	data &port_name._holdings;
		set &port_name._holdings;
			year	= year(date);
			month 	= month(date);
	run;

	data &port_name._holdings;
		merge &port_name._holdings(in=k) &price_data(keep = date &id mkt_cap lag_mkt_cap);
			by date &id;
			if k;
			if missing(mkt_cap) = 0;
	run;

	proc means
		data 	= &port_name._holdings noprint;
		var 	mkt_cap lag_mkt_cap;
		by 		date;
		output
			out	= &port_name._holdings_sums
			sum = mkt_cap_sum lag_mkt_cap_sum;
	run;

	data &port_name._holdings(drop = _TYPE_ _FREQ_ mkt_cap_sum lag_mkt_cap_sum);
		merge &port_name._holdings(in=k) &port_name._holdings_sums;
			by date;
			if k;
		
		if missing(mkt_cap) = 0 then do;
			wt_cap = mkt_cap / mkt_cap_sum;
		end;

		if missing(lag_mkt_cap) = 0 then do;
			lag_wt_cap = lag_mkt_cap / lag_mkt_cap_sum;
		end;

		wt_eq = 1 / _FREQ_;
	run;

	proc datasets library=work nolist;
  		modify &port_name._holdings;
  		attrib _all_ label='';
	quit;
	
	/*
	data port.&port_name._holdings;	
		set &port_name._holdings;
	run;
	*/

	/* clean-up */

	proc datasets 
		library		= work;
   		delete 		&port_name._holdings_sums;
	run;

	proc datasets 
		library		= work;
   		delete 		date_window;
	run;

	proc datasets 
		library		= work;
   		delete 		temp_hold;
	run;

	proc datasets 
		library		= work;
   		delete 		holdings;
	run;
	quit;

%mend;


/* port_ret: computes returns given a sequence of weights */

%macro port_returns(port_data, price_data) / store source;

	data &port_data;
		merge &port_data(in=k) &price_data(keep = date permno ret retx);
			by date permno;
			if k;
			tot_ret = lag_wt_cap*ret;
	run;

	proc means
		data	= &port_data noprint;
		var		tot_ret;
		by		year month;
	output
		out		= &port_data._ret
		sum		= port_ret;
	run;

%mend;

/* compute_returns: a second, more efficient means of computing portfolio returns */

%macro compute_returns(port_data, signal, rebal_mon, n_groups, signal_group) / store source;

	/* rank signal */
	proc sort
		data = &port_data;
		by year month &signal;
	run;

	proc rank
		data 	= &port_data
		out		= &port_data
		groups	= &n_groups;
		by 		year month;
		var		&signal;
		ranks   signal_rank;
	run;

	/* map weight measure to obs matching signal rank and rebalance month */
	data &port_data;
		set &port_data;
			if signal_rank = &signal_group & month = &rebal_mon then
				wt = 1;
	run;

	proc sort
		data = &port_data;
		by permno year month;
	run;

	/* compute portfolio weights */
	data &port_data;
		set &port_data;
			by permno year month;
			retain dyn_wt;
			if first.permno then do;
				lag_ret  = .;
				lag_retx = .;
			end;
			else do;
				lag_ret  = lag(ret);
				lag_retx = lag(retx);
			end;
			if month = &rebal_mon then 
				dyn_wt = wt;
			else
				dyn_wt = dyn_wt*(1 + lag_retx);

	run;
	
	proc sort
		data = &port_data;
		by year month;
	run;

	proc means
		data 	= &port_data noprint;
		var 	dyn_wt;
		by 		year month;
		output
			out	= wt_sums
			sum = dyn_wt_sum;
	run;

	data &port_data(drop = dyn_wt_sum);
		merge &port_data wt_sums(drop = _TYPE_ _FREQ_);
			by year month;
				if missing(dyn_wt) = 0;
				port_wt 	= dyn_wt/dyn_wt_sum;
				port_ret	= port_wt*ret;
				port_retx 	= port_wt*retx;
	run;

	proc means 
		data 	= &port_data noprint;
		var 	port_ret port_retx;
		by 		year month;
		output 
			out = &signal._return_&signal_group
		 	sum = &signal._&signal_group._TR &signal._&signal_group._PR;
	run;


%mend;
