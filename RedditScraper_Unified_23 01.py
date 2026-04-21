"""
Reddit Scraper — Unified v9
Three scrape modes in one window:
  • Subreddit  — browse a specific subreddit (PRAW)
  • Search     — keyword search across all Reddit (PRAW)
  • Arctic Shift — historical archive, any date range (no PRAW needed)
Shared: pain score, sentiment, txt digest writer, all filters, dedup.
"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading, json, os, subprocess, sys, time, re, urllib.request, urllib.parse
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    import praw;  PRAW_OK = True
except ImportError:
    PRAW_OK = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    analyser = SentimentIntensityAnalyzer(); VADER_OK = True
except ImportError:
    VADER_OK = False

try:
    from win10toast import ToastNotifier; TOAST_OK = True
except ImportError:
    TOAST_OK = False

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "reddit_unified_config.json")
DEFAULT_CONFIG = {
    "client_id":     os.environ.get("REDDIT_CLIENT_ID", ""),
    "client_secret": os.environ.get("REDDIT_CLIENT_SECRET", ""),
    "user_agent":    os.environ.get("REDDIT_USER_AGENT", "UnifiedScraper"),
    # Subreddit mode
    "subreddit":     "",
    "post_type":     "top",
    # Search mode
    "query":         "",
    "sort":          "relevance",
    # Arctic Shift mode
    "as_query":      "",
    "as_subreddit":  "",
    "as_date_from":  "",
    "as_date_to":    "",
    "as_sort":       "desc",
    # Shared
    "time_filter":   "year",
    "post_limit":    "200",
    "comment_depth": "2",
    "max_comments":  "30",
    "min_comments":  "0",
    "min_upvotes":   "0",
    "min_ratio":     "0",
    "min_resonance": "0",
    "max_resonance": "100",
    "output_dir":    os.path.join(os.path.expanduser("~"), "RedditScrapping"),
    "mode":          "subreddit",
}

PRESETS = {
    "Quick Scan":     {"post_limit": "100", "comment_depth": "0"},
    "Standard":       {"post_limit": "300", "comment_depth": "2"},
    "Deep Dive":      {"post_limit": "1000", "comment_depth": "5"},
    "Custom":         {},
}

# ── Palette ───────────────────────────────────────────────────────────────────
BG=      "#212121"; SIDEBAR="#171717"; PANEL=  "#2f2f2f"
ACCENT=  "#10a37f"; ACCENT2="#0d8f6e"; REDDIT= "#ff4500"
TEXT=    "#ececec"; SUBTEXT="#8e8ea0"; MUTED=  "#565869"
SUCCESS= "#10a37f"; WARNING="#f5a623"; ERROR=  "#ef4444"
BORDER=  "#3d3d3d"; INPUT_BG="#404040";HOVER=  "#4a4a4a"
ARCTIC=  "#5b8dd9"   # blue accent for Arctic Shift mode

FN_HEAD=("Segoe UI Semibold",14,"bold"); FN_SUB= ("Segoe UI",9)
FN_ENTRY=("Segoe UI",10);                FN_LOG= ("Consolas",9)
FN_BTN= ("Segoe UI Semibold",9);         FN_SMALL=("Segoe UI",8)
FN_MONO=("Consolas",9)

# ── Pure helpers ──────────────────────────────────────────────────────────────
def load_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH) as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception: pass
    return dict(DEFAULT_CONFIG)

def save_config(cfg):
    try:
        with open(CONFIG_PATH,"w") as f: json.dump(cfg,f,indent=2)
    except Exception: pass

def _open_file(path):
    if sys.platform == "win32":
        os.startfile(path)
    elif sys.platform == "darwin":
        subprocess.run(["open", path])
    else:
        subprocess.run(["xdg-open", path])

def get_sentiment(text):
    if not VADER_OK or not text or text in ("[No comments]","[Link Post]"):
        return "Neutral"
    s = analyser.polarity_scores(text)["compound"]
    return "Positive" if s>=0.05 else ("Negative" if s<=-0.05 else "Neutral")

def fetch_with_retry(fn, retries=3, backoff=2.0, log_fn=None):
    for attempt in range(retries):
        try: return fn()
        except Exception as e:
            wait = backoff**attempt
            if log_fn: log_fn(f"  ⚠ Attempt {attempt+1} failed ({e}). Retrying in {wait:.0f}s…","warn")
            time.sleep(wait)
    raise RuntimeError(f"All {retries} attempts failed.")

ILLEGAL_XML = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
def sanitize(val):
    if not isinstance(val,str): return val
    return ILLEGAL_XML.sub('', val)[:32764] + ("..." if len(val)>32767 else "")

def clean_sub(raw):
    s = raw.strip()
    for p in ("/r/","r/"):
        if s.lower().startswith(p): s=s[len(p):]
    return s.strip()

def calc_pain_score(post_ts, comment_list, post_score, post_n_comments, post_sentiment):
    cph_1h = 0.0
    if comment_list:
        ctimes=[]
        for c in comment_list:
            try: ctimes.append(datetime.strptime(c["date"],"%Y-%m-%d %H:%M").timestamp())
            except: pass
        if ctimes:
            age_h = max((datetime.utcnow().timestamp()-post_ts)/3600,0.01)
            c1h   = sum(1 for t in ctimes if t<=post_ts+3600)
            cph_1h= c1h/min(age_h,1)

    sent_w   = {"Negative":1.5,"Neutral":1.0,"Positive":0.6}.get(post_sentiment,1.0)
    upvotes  = max(post_score,1)
    eng_ratio= post_n_comments/upvotes
    eng_score= min(30, eng_ratio*60)
    vpen     = 0.2 if post_score>50000 else 0.5 if post_score>10000 else 0.8 if post_score>1000 else 1.0
    d1       = sum(1 for c in comment_list if c.get("depth",0)==1)
    depth_sc = min(20,(d1/max(len(comment_list),1))*40)
    vel_sc   = min(30,cph_1h*2)*vpen
    pain     = min(100,int((vel_sc+eng_score+depth_sc)*sent_w))
    pattern  = ("STRONG PAIN" if pain>=70 else "CLEAR PAIN" if pain>=45
                else "MILD PAIN" if pain>=25 else "WEAK SIGNAL" if pain>=10 else "NOISE")
    return {"score":pain,"pattern":pattern,"cph_1h":round(cph_1h,1),
            "eng_ratio":round(eng_ratio,3),"viral_pen":vpen,
            "depth_ratio":round(d1/max(len(comment_list),1),2)}

def _hsep(parent,color):
    return tk.Frame(parent,bg=color,height=1)

# ── _Btn widget ───────────────────────────────────────────────────────────────
class _Btn(tk.Frame):
    def __init__(self,parent,text,cmd,bg,hover_bg):
        super().__init__(parent,bg=bg,cursor="hand2",
                         highlightbackground=BORDER,highlightthickness=1)
        self._cmd=cmd; self._enabled=True; self._bg=bg; self._hbg=hover_bg
        self._lbl=tk.Label(self,text=text,font=FN_BTN,bg=bg,fg=TEXT,padx=14,pady=7,cursor="hand2")
        self._lbl.pack()
        for w in (self,self._lbl):
            w.bind("<Button-1>",self._click)
            w.bind("<Enter>",   self._enter)
            w.bind("<Leave>",   self._leave)
    def _click(self,e=None):
        if self._enabled: self._cmd()
    def _enter(self,e=None):
        if self._enabled: self.config(bg=self._hbg); self._lbl.config(bg=self._hbg)
    def _leave(self,e=None):
        self.config(bg=self._bg); self._lbl.config(bg=self._bg)
    def set_enabled(self,val):
        self._enabled=val
        self._lbl.config(fg=TEXT if val else MUTED,cursor="hand2" if val else "arrow")
        self.config(cursor="hand2" if val else "arrow")

# ─────────────────────────────────────────────────────────────────────────────
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.cfg         = load_config()
        self._stop_flag  = threading.Event()
        self._is_running = False
        self._val_job    = None
        self._mode       = tk.StringVar(value=self.cfg.get("mode","subreddit"))

        self.title("Reddit Scraper  v9")
        self.configure(bg=BG)
        self.minsize(980,660)
        self.geometry("1140x760")

        self._setup_ttk()
        self._build()
        self._load_ui()
        self.after(300,self._check_deps)

    # ── TTK styles ────────────────────────────────────────────────────────────
    def _setup_ttk(self):
        s=ttk.Style(); s.theme_use("clam")
        s.configure("G.Horizontal.TProgressbar",
                    troughcolor=SIDEBAR,background=ACCENT,
                    darkcolor=ACCENT,lightcolor=ACCENT2,bordercolor=SIDEBAR,thickness=4)
        s.configure("A.Horizontal.TProgressbar",
                    troughcolor=SIDEBAR,background=ARCTIC,
                    darkcolor=ARCTIC,lightcolor=ARCTIC,bordercolor=SIDEBAR,thickness=4)
        for name in ("G","A"):
            s.configure(f"{name}.TCombobox",fieldbackground=INPUT_BG,background=INPUT_BG,
                        foreground=TEXT,selectbackground=ACCENT,selectforeground=TEXT,
                        bordercolor=BORDER,arrowcolor=SUBTEXT)
            s.map(f"{name}.TCombobox",fieldbackground=[("readonly",INPUT_BG)],
                  foreground=[("readonly",TEXT)],selectbackground=[("readonly",ACCENT)])

    # ── Layout ────────────────────────────────────────────────────────────────
    def _build(self):
        self.columnconfigure(0,weight=0,minsize=290)
        self.columnconfigure(1,weight=1)
        self.rowconfigure(0,weight=1)
        self._build_sidebar()
        self._build_main()

    # ─────────────────── SIDEBAR ─────────────────────────────────────────────
    def _build_sidebar(self):
        # Outer container holds logo+tabs (fixed) and scrollable content
        outer=tk.Frame(self,bg=SIDEBAR,width=290)
        outer.grid(row=0,column=0,sticky="nsew")
        outer.grid_propagate(False)
        outer.columnconfigure(0,weight=1)
        outer.rowconfigure(1,weight=1)
        self._sb_outer=outer

        # ── Fixed top: logo + mode tabs ───────────────────────────────────────
        top=tk.Frame(outer,bg=SIDEBAR)
        top.grid(row=0,column=0,sticky="ew")
        top.columnconfigure(0,weight=1)

        logo=tk.Frame(top,bg=SIDEBAR)
        logo.grid(row=0,column=0,sticky="ew",padx=18,pady=(18,10))
        tk.Label(logo,text="Reddit",font=FN_HEAD,bg=SIDEBAR,fg=REDDIT).pack(side="left")
        tk.Label(logo,text=" Scraper",font=FN_HEAD,bg=SIDEBAR,fg=TEXT).pack(side="left")
        tk.Label(logo,text="  v9",font=FN_SMALL,bg=SIDEBAR,fg=MUTED).pack(side="left",pady=(5,0))

        _hsep(top,BORDER).grid(row=1,column=0,sticky="ew",padx=14,pady=(0,6))

        tabs=tk.Frame(top,bg=SIDEBAR)
        tabs.grid(row=2,column=0,sticky="ew",padx=14,pady=(0,8))
        for col in range(3): tabs.columnconfigure(col,weight=1)
        self._mode_tabs={}
        for i,(m,label) in enumerate([("subreddit","Subreddit"),
                                       ("search","Search"),
                                       ("arctic","Arctic Shift")]):
            pill=tk.Label(tabs,text=label,font=FN_SMALL,bg=INPUT_BG,fg=SUBTEXT,
                          padx=6,pady=5,cursor="hand2",relief="flat",
                          highlightbackground=BORDER,highlightthickness=1)
            pill.grid(row=0,column=i,sticky="ew",padx=(0,3))
            pill.bind("<Button-1>",lambda e,mv=m: self._switch_mode(mv))
            self._mode_tabs[m]=pill

        _hsep(top,BORDER).grid(row=3,column=0,sticky="ew",padx=14,pady=(0,6))

        # ── Scrollable content area ───────────────────────────────────────────
        scroll_container=tk.Frame(outer,bg=SIDEBAR)
        scroll_container.grid(row=1,column=0,sticky="nsew")
        scroll_container.columnconfigure(0,weight=1)
        scroll_container.rowconfigure(0,weight=1)

        canvas=tk.Canvas(scroll_container,bg=SIDEBAR,highlightthickness=0,bd=0)
        canvas.grid(row=0,column=0,sticky="nsew")

        sb_scroll=tk.Scrollbar(scroll_container,orient="vertical",command=canvas.yview)
        sb_scroll.grid(row=0,column=1,sticky="ns")
        canvas.configure(yscrollcommand=sb_scroll.set)

        # Inner scrollable frame
        self._sb=tk.Frame(canvas,bg=SIDEBAR)
        self._sb.columnconfigure(0,weight=1)
        canvas_window=canvas.create_window((0,0),window=self._sb,anchor="nw")

        def on_frame_configure(e):
            canvas.configure(scrollregion=canvas.bbox("all"))
        def on_canvas_configure(e):
            canvas.itemconfig(canvas_window,width=e.width)
        def on_mousewheel(e):
            canvas.yview_scroll(int(-1*(e.delta/120)),"units")

        self._sb.bind("<Configure>",on_frame_configure)
        canvas.bind("<Configure>",on_canvas_configure)
        canvas.bind("<MouseWheel>",on_mousewheel)
        self._sb.bind("<MouseWheel>",on_mousewheel)

        # ── Mode-specific panels ───────────────────────────────────────────────
        self._panels={}
        self._panels["subreddit"] = self._make_subreddit_panel()
        self._panels["search"]    = self._make_search_panel()
        self._panels["arctic"]    = self._make_arctic_panel()

        # ── Shared settings ────────────────────────────────────────────────────
        self._shared_frame = tk.Frame(self._sb,bg=SIDEBAR)
        self._shared_frame.grid(row=5,column=0,sticky="ew")
        self._shared_frame.columnconfigure(0,weight=1)
        self._build_shared(self._shared_frame)

        # Bind mousewheel to all child widgets recursively
        def bind_mousewheel(widget):
            widget.bind("<MouseWheel>",on_mousewheel)
            for child in widget.winfo_children():
                bind_mousewheel(child)
        self._sb.after(200, lambda: bind_mousewheel(self._sb))

    def _make_subreddit_panel(self):
        f=tk.Frame(self._sb,bg=SIDEBAR); f.columnconfigure(0,weight=1)

        self._slbl(f,"Subreddit",0)
        sr=tk.Frame(f,bg=SIDEBAR); sr.grid(row=1,column=0,sticky="ew",padx=14,pady=(0,8))
        sr.columnconfigure(0,weight=1)
        self.var_sub=tk.StringVar()
        self.var_sub.trace_add("write",self._on_sub_change)
        self._entry_frame(sr,self.var_sub,row=0,col=0)
        self.lbl_valid=tk.Label(sr,text="",font=("Segoe UI",11),bg=SIDEBAR,fg=SUBTEXT,width=2)
        self.lbl_valid.grid(row=0,column=1,padx=(5,0))

        self._slbl(f,"Preset",2)
        self.var_preset=tk.StringVar(value="Custom")
        cb=ttk.Combobox(f,textvariable=self.var_preset,values=list(PRESETS.keys()),
                        state="readonly",font=FN_ENTRY,style="G.TCombobox")
        cb.grid(row=3,column=0,sticky="ew",padx=14,pady=(0,8))
        cb.bind("<<ComboboxSelected>>",self._apply_preset)

        self._slbl(f,"Post Type",4)
        pt_frame=tk.Frame(f,bg=SIDEBAR); pt_frame.grid(row=5,column=0,sticky="ew",padx=14,pady=(0,8))
        self.var_post_type=tk.StringVar(value="top")
        self._pt_btns={}
        for col in range(3): pt_frame.columnconfigure(col,weight=1)
        for i,pt in enumerate(["new","hot","rising","top","controversial"]):
            pill=tk.Label(pt_frame,text=pt.capitalize(),font=FN_ENTRY,
                          bg=INPUT_BG,fg=SUBTEXT,padx=8,pady=5,cursor="hand2",relief="flat",
                          highlightbackground=BORDER,highlightthickness=1)
            pill.grid(row=i//3,column=i%3,sticky="ew",padx=(0,3),pady=(0,3))
            pill.bind("<Button-1>",lambda e,p=pt: self._pick_type(p))
            self._pt_btns[pt]=pill

        self._slbl(f,"Time Filter",6)
        self.var_time=tk.StringVar(value="year")
        self.cb_time=ttk.Combobox(f,textvariable=self.var_time,
                                  values=["all","day","hour","month","week","year"],
                                  state="readonly",font=FN_ENTRY,style="G.TCombobox")
        self.cb_time.grid(row=7,column=0,sticky="ew",padx=14,pady=(0,8))
        return f

    def _make_search_panel(self):
        f=tk.Frame(self._sb,bg=SIDEBAR); f.columnconfigure(0,weight=1)

        self._slbl(f,"Search Query",0)
        self.var_query=tk.StringVar()
        self._entry_frame(f,self.var_query,row=1,col=0,pad=True)
        tk.Label(f,text="searches all of Reddit  (no subreddit filter)",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED
                 ).grid(row=2,column=0,sticky="w",padx=14,pady=(0,6))

        self._slbl(f,"Sort By",3)
        sf=tk.Frame(f,bg=SIDEBAR); sf.grid(row=4,column=0,sticky="ew",padx=14,pady=(0,8))
        self.var_sort=tk.StringVar(value="relevance")
        self._sort_btns={}
        for col in range(3): sf.columnconfigure(col,weight=1)
        for i,s in enumerate(["relevance","hot","top","new","comments"]):
            pill=tk.Label(sf,text=s.capitalize(),font=FN_ENTRY,
                          bg=INPUT_BG,fg=SUBTEXT,padx=8,pady=5,cursor="hand2",relief="flat",
                          highlightbackground=BORDER,highlightthickness=1)
            pill.grid(row=i//3,column=i%3,sticky="ew",padx=(0,3),pady=(0,3))
            pill.bind("<Button-1>",lambda e,sv=s: self._pick_sort(sv))
            self._sort_btns[s]=pill

        self._slbl(f,"Time Filter (PRAW)",5)
        self.var_search_time=tk.StringVar(value="year")
        ttk.Combobox(f,textvariable=self.var_search_time,
                     values=["all","day","hour","month","week","year"],
                     state="readonly",font=FN_ENTRY,style="G.TCombobox"
                     ).grid(row=6,column=0,sticky="ew",padx=14,pady=(0,8))
        return f

    def _make_arctic_panel(self):
        f=tk.Frame(self._sb,bg=SIDEBAR); f.columnconfigure(0,weight=1)

        # Arctic branding
        ab=tk.Frame(f,bg=SIDEBAR)
        ab.grid(row=0,column=0,sticky="ew",padx=14,pady=(0,8))
        tk.Label(ab,text="❄",font=("Segoe UI",14),bg=SIDEBAR,fg=ARCTIC).pack(side="left")
        tk.Label(ab,text=" arctic-shift.photon-reddit.com",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED).pack(side="left",pady=(4,0))

        self._slbl(f,"Keyword / Query",1)
        self.var_as_query=tk.StringVar()
        self._entry_frame(f,self.var_as_query,row=2,col=0,pad=True)

        self._slbl(f,"Subreddit  (optional)",3)
        self.var_as_sub=tk.StringVar()
        self._entry_frame(f,self.var_as_sub,row=4,col=0,pad=True)
        tk.Label(f,text="leave blank to search all of Reddit",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED
                 ).grid(row=5,column=0,sticky="w",padx=14,pady=(0,6))

        self._slbl(f,"Date From",6)
        from datetime import date, timedelta
        _today    = date.today().strftime("%Y-%m-%d")
        _one_year = (date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.var_as_from=tk.StringVar(value=_one_year)
        self._entry_frame(f,self.var_as_from,row=7,col=0,pad=True)

        self._slbl(f,"Date To",8)
        self.var_as_to=tk.StringVar(value=_today)
        self._entry_frame(f,self.var_as_to,row=9,col=0,pad=True)

        self._slbl(f,"Sort Order",10)
        asf=tk.Frame(f,bg=SIDEBAR); asf.grid(row=11,column=0,sticky="ew",padx=14,pady=(0,8))
        self.var_as_sort=tk.StringVar(value="desc")
        self._as_sort_btns={}
        for col in range(3): asf.columnconfigure(col,weight=1)
        for i,s in enumerate(["desc","asc"]):
            label={"desc":"Newest first","asc":"Oldest first"}[s]
            pill=tk.Label(asf,text=label,font=FN_ENTRY,
                          bg=INPUT_BG,fg=SUBTEXT,padx=8,pady=5,cursor="hand2",relief="flat",
                          highlightbackground=BORDER,highlightthickness=1)
            pill.grid(row=0,column=i,sticky="ew",padx=(0,3))
            pill.bind("<Button-1>",lambda e,sv=s: self._pick_as_sort(sv))
            self._as_sort_btns[s]=pill

        tk.Label(f,text="Note: Arctic Shift fetches comments separately per post.",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED,wraplength=240,justify="left"
                 ).grid(row=12,column=0,sticky="w",padx=14,pady=(4,8))
        return f

    def _build_shared(self,parent):
        _hsep(parent,BORDER).grid(row=0,column=0,sticky="ew",padx=14,pady=(4,6))

        self._slbl(parent,"Post Limit",1)
        self.var_limit=tk.StringVar(value="200")
        self._entry_frame(parent,self.var_limit,row=2,col=0,pad=True)

        self._slbl(parent,"Comment Depth",3)
        df=tk.Frame(parent,bg=SIDEBAR); df.grid(row=4,column=0,sticky="ew",padx=14,pady=(0,8))
        df.columnconfigure(0,weight=1)
        self.var_depth=tk.IntVar(value=2)
        tk.Scale(df,from_=0,to=10,orient="horizontal",variable=self.var_depth,showvalue=False,
                 bg=SIDEBAR,fg=TEXT,troughcolor=INPUT_BG,activebackground=ACCENT,
                 highlightthickness=0,sliderlength=14,width=5).grid(row=0,column=0,sticky="ew")
        tk.Label(df,textvariable=self.var_depth,font=("Segoe UI Semibold",10),
                 bg=SIDEBAR,fg=ACCENT,width=2).grid(row=0,column=1,padx=(6,0))

        self._slbl(parent,"Max Comments / Post",5)
        self.var_max_cmt=tk.StringVar(value="30")
        self._entry_frame(parent,self.var_max_cmt,row=6,col=0,pad=True)
        tk.Label(parent,text="top comments written to digest  (30 recommended)",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED
                 ).grid(row=7,column=0,sticky="w",padx=14,pady=(0,6))

        self._slbl(parent,"Output Folder",8)
        of=tk.Frame(parent,bg=SIDEBAR)
        of.grid(row=9,column=0,sticky="ew",padx=14,pady=(0,8))
        of.columnconfigure(0,weight=1)
        self.var_outdir=tk.StringVar()
        self._entry_frame(of,self.var_outdir,row=0,col=0)
        browse=tk.Label(of,text="…",font=("Segoe UI",11),bg=INPUT_BG,fg=TEXT,
                        padx=9,pady=5,cursor="hand2",relief="flat")
        browse.grid(row=0,column=1,padx=(4,0))
        browse.bind("<Button-1>",lambda e: self._browse())
        browse.bind("<Enter>",lambda e: browse.config(bg=HOVER))
        browse.bind("<Leave>",lambda e: browse.config(bg=INPUT_BG))

        # Filters
        _hsep(parent,BORDER).grid(row=10,column=0,sticky="ew",padx=14,pady=(6,4))
        tk.Label(parent,text="FILTERS  (skip posts below / above threshold)",
                 font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED
                 ).grid(row=11,column=0,sticky="w",padx=14,pady=(0,6))
        fg=tk.Frame(parent,bg=SIDEBAR)
        fg.grid(row=12,column=0,sticky="ew",padx=14,pady=(0,14))
        fg.columnconfigure(1,weight=1)

        self.var_min_comments  = tk.StringVar(value="0")
        self.var_min_upvotes   = tk.StringVar(value="0")
        self.var_min_ratio     = tk.StringVar(value="0")
        self.var_min_resonance = tk.StringVar(value="0")
        self.var_max_resonance = tk.StringVar(value="100")

        for ri,(label,var) in enumerate([
            ("Min comments",  self.var_min_comments),
            ("Min upvotes",   self.var_min_upvotes),
            ("Min upv ratio", self.var_min_ratio),
            ("Min pain score",self.var_min_resonance),
            ("Max pain score",self.var_max_resonance),
        ]):
            tk.Label(fg,text=label,font=FN_SMALL,bg=SIDEBAR,fg=SUBTEXT,
                     anchor="w",width=14).grid(row=ri,column=0,sticky="w",pady=3)
            ff=tk.Frame(fg,bg=INPUT_BG,highlightbackground=BORDER,highlightthickness=1)
            ff.grid(row=ri,column=1,sticky="ew",padx=(6,0),pady=3)
            ent=tk.Entry(ff,textvariable=var,font=FN_ENTRY,bg=INPUT_BG,fg=TEXT,
                         insertbackground=TEXT,relief="flat",bd=4,width=7)
            ent.pack(fill="x")
            ent.bind("<FocusIn>", lambda e,ff=ff: ff.config(highlightbackground=ACCENT))
            ent.bind("<FocusOut>",lambda e,ff=ff: ff.config(highlightbackground=BORDER))

    # ── MAIN AREA ─────────────────────────────────────────────────────────────
    def _build_main(self):
        main=tk.Frame(self,bg=BG)
        main.grid(row=0,column=1,sticky="nsew")
        main.columnconfigure(0,weight=1)
        main.rowconfigure(1,weight=1)

        top=tk.Frame(main,bg=BG,padx=20,pady=12)
        top.grid(row=0,column=0,sticky="ew")
        top.columnconfigure(1,weight=1)
        self.lbl_mode=tk.Label(top,text="Activity Log — Subreddit Mode",
                               font=("Segoe UI Semibold",12),bg=BG,fg=TEXT)
        self.lbl_mode.grid(row=0,column=0,sticky="w")
        self.status_pill=tk.Label(top,text="● Idle",font=FN_SMALL,bg=PANEL,fg=MUTED,padx=10,pady=4)
        self.status_pill.grid(row=0,column=2,sticky="e")
        _hsep(main,BORDER).grid(row=0,column=0,sticky="sew")

        self.log_box=scrolledtext.ScrolledText(
            main,font=FN_LOG,bg=BG,fg=TEXT,insertbackground=TEXT,
            relief="flat",wrap="word",state="disabled",
            selectbackground=ACCENT,bd=0,spacing1=3,spacing3=3)
        self.log_box.grid(row=1,column=0,sticky="nsew",padx=20,pady=(14,0))
        for tag,clr in [("warn",WARNING),("error",ERROR),("success",SUCCESS),
                        ("accent",ACCENT),("arctic",ARCTIC),("sub",MUTED),("ts",MUTED)]:
            self.log_box.tag_config(tag,foreground=clr)

        cards=tk.Frame(main,bg=BG)
        cards.grid(row=2,column=0,sticky="ew",padx=20,pady=12)
        for i in range(3): cards.columnconfigure(i,weight=1)
        self.stat_vars={}
        for lbl,clr,r,c in [
            ("Scraped", ACCENT,    0,0),("Skipped", SUBTEXT,   0,1),("Comments","#7c8cf8",0,2),
            ("Positive",SUCCESS,   1,0),("Neutral", SUBTEXT,   1,1),("Negative",ERROR,    1,2),
        ]:
            self.stat_vars[lbl]=tk.StringVar(value="0")
            card=tk.Frame(cards,bg=PANEL,highlightbackground=BORDER,highlightthickness=1)
            card.grid(row=r,column=c,sticky="ew",padx=(0,0 if c==2 else 8),pady=(8 if r==1 else 0,0))
            inner=tk.Frame(card,bg=PANEL,padx=12,pady=8); inner.pack()
            tk.Label(inner,textvariable=self.stat_vars[lbl],
                     font=("Segoe UI Semibold",18),bg=PANEL,fg=clr).pack()
            tk.Label(inner,text=lbl,font=FN_SMALL,bg=PANEL,fg=MUTED).pack()

        # ── 7th card: Date Progress ───────────────────────────────────────────
        date_card=tk.Frame(cards,bg=PANEL,highlightbackground=BORDER,highlightthickness=1)
        date_card.grid(row=2,column=0,columnspan=3,sticky="ew",pady=(8,0))
        date_card.columnconfigure(1,weight=1)

        tk.Label(date_card,text="DATE PROGRESS",font=FN_SMALL,bg=PANEL,fg=MUTED,
                 anchor="w").grid(row=0,column=0,sticky="w",padx=(14,0),pady=(8,2))
        self.var_date_progress=tk.StringVar(value="—")
        tk.Label(date_card,textvariable=self.var_date_progress,
                 font=("Segoe UI Semibold",10),bg=PANEL,fg=ARCTIC,anchor="w"
                 ).grid(row=0,column=1,sticky="w",padx=(8,0),pady=(8,2))
        self.var_date_pct=tk.StringVar(value="")
        tk.Label(date_card,textvariable=self.var_date_pct,
                 font=FN_SMALL,bg=PANEL,fg=MUTED,anchor="e"
                 ).grid(row=0,column=2,sticky="e",padx=(0,14),pady=(8,2))
        self.date_progressbar=ttk.Progressbar(date_card,
                                               style="A.Horizontal.TProgressbar",
                                               orient="horizontal",mode="determinate",
                                               maximum=100)
        self.date_progressbar.grid(row=1,column=0,columnspan=3,
                                   sticky="ew",padx=14,pady=(0,8))

        _hsep(main,BORDER).grid(row=3,column=0,sticky="ew")
        bot=tk.Frame(main,bg=BG,padx=20,pady=12)
        bot.grid(row=4,column=0,sticky="ew")
        bot.columnconfigure(2,weight=1)

        self.btn_start=_Btn(bot,"▶  Run",self._start,ACCENT,ACCENT2)
        self.btn_start.grid(row=0,column=0,padx=(0,8))
        self.btn_stop=_Btn(bot,"■  Stop",self._stop,PANEL,HOVER)
        self.btn_stop.grid(row=0,column=1)
        self.btn_stop.set_enabled(False)

        pf=tk.Frame(bot,bg=BG); pf.grid(row=0,column=2,sticky="ew",padx=(18,0))
        pf.columnconfigure(0,weight=1)
        self.prog_lbl=tk.Label(pf,text="Ready",font=FN_SMALL,bg=BG,fg=MUTED,anchor="w")
        self.prog_lbl.grid(row=0,column=0,sticky="w")
        self.progressbar=ttk.Progressbar(pf,style="G.Horizontal.TProgressbar",
                                         orient="horizontal",mode="determinate")
        self.progressbar.grid(row=1,column=0,sticky="ew",pady=(4,0))

    # ── Widget helpers ────────────────────────────────────────────────────────
    def _slbl(self,parent,text,row):
        tk.Label(parent,text=text.upper(),font=("Segoe UI",7),bg=SIDEBAR,fg=MUTED
                 ).grid(row=row,column=0,sticky="w",padx=14,pady=(10,3))

    def _entry_frame(self,parent,var,row,col,pad=False):
        f=tk.Frame(parent,bg=INPUT_BG,highlightbackground=BORDER,highlightthickness=1)
        kw={"row":row,"column":col,"sticky":"ew"}
        if pad: kw.update({"padx":14,"pady":(0,10)})
        f.grid(**kw); f.columnconfigure(0,weight=1)
        ent=tk.Entry(f,textvariable=var,font=FN_ENTRY,bg=INPUT_BG,fg=TEXT,
                     insertbackground=TEXT,relief="flat",bd=6)
        ent.grid(row=0,column=0,sticky="ew")
        ent.bind("<FocusIn>", lambda e: f.config(highlightbackground=ACCENT))
        ent.bind("<FocusOut>",lambda e: f.config(highlightbackground=BORDER))
        return f,ent

    def _pick_type(self,pt):
        self.var_post_type.set(pt)
        for n,pill in self._pt_btns.items():
            pill.config(bg=ACCENT if n==pt else INPUT_BG,
                        fg=TEXT   if n==pt else SUBTEXT,
                        highlightbackground=ACCENT if n==pt else BORDER)
        self.cb_time.config(state="readonly" if pt in ("top","controversial") else "disabled")

    def _pick_sort(self,s):
        self.var_sort.set(s)
        for n,pill in self._sort_btns.items():
            pill.config(bg=ACCENT if n==s else INPUT_BG,
                        fg=TEXT   if n==s else SUBTEXT,
                        highlightbackground=ACCENT if n==s else BORDER)

    def _pick_as_sort(self,s):
        self.var_as_sort.set(s)
        for n,pill in self._as_sort_btns.items():
            pill.config(bg=ARCTIC if n==s else INPUT_BG,
                        fg=TEXT   if n==s else SUBTEXT,
                        highlightbackground=ARCTIC if n==s else BORDER)

    def _switch_mode(self,mode):
        self._mode.set(mode)
        # Hide all panels
        for m,panel in self._panels.items():
            panel.grid_forget()
        # Show selected
        self._panels[mode].grid(row=4,column=0,sticky="ew")
        # Update mode tab highlight
        for m,pill in self._mode_tabs.items():
            clr = ARCTIC if m=="arctic" else ACCENT
            pill.config(bg=clr if m==mode else INPUT_BG,
                        fg=TEXT if m==mode else SUBTEXT,
                        highlightbackground=clr if m==mode else BORDER)
        # Update progressbar style and log header
        style = "A.Horizontal.TProgressbar" if mode=="arctic" else "G.Horizontal.TProgressbar"
        self.progressbar.config(style=style)
        labels={"subreddit":"Subreddit Mode","search":"Search Mode","arctic":"Arctic Shift Mode"}
        self.lbl_mode.config(text=f"Activity Log — {labels[mode]}")

    def _browse(self):
        d=filedialog.askdirectory(initialdir=self.var_outdir.get())
        if d: self.var_outdir.set(d)

    # ── Subreddit validation ──────────────────────────────────────────────────
    def _on_sub_change(self,*_):
        if self._val_job: self.after_cancel(self._val_job)
        self.lbl_valid.config(text="",fg=SUBTEXT)
        raw=self.var_sub.get(); cleaned=clean_sub(raw)
        if cleaned!=raw: self.var_sub.set(cleaned); return
        if cleaned: self._val_job=self.after(900,lambda: self._validate(cleaned))

    def _validate(self,name):
        if not PRAW_OK: return
        def run():
            try:
                r=praw.Reddit(client_id=self.cfg["client_id"],
                              client_secret=self.cfg["client_secret"],
                              user_agent=self.cfg["user_agent"])
                _ = r.subreddit(name).title
                self.after(0,lambda: self.lbl_valid.config(text="✓",fg=SUCCESS))
            except:
                self.after(0,lambda: self.lbl_valid.config(text="✗",fg=ERROR))
        threading.Thread(target=run,daemon=True).start()

    # ── Config IO ─────────────────────────────────────────────────────────────
    def _load_ui(self):
        c=self.cfg
        self.var_sub.set(c.get("subreddit",""))
        self._pick_type(c.get("post_type","top"))
        self.var_time.set(c.get("time_filter","year"))
        self.var_query.set(c.get("query",""))
        self._pick_sort(c.get("sort","relevance"))
        self.var_search_time.set(c.get("time_filter","year"))
        self.var_as_query.set(c.get("as_query",""))
        self.var_as_sub.set(c.get("as_subreddit",""))
        from datetime import date, timedelta
        today    = date.today().strftime("%Y-%m-%d")
        one_year = (date.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        self.var_as_from.set(c.get("as_date_from","") or one_year)
        self.var_as_to.set(c.get("as_date_to","")   or today)
        self._pick_as_sort(c.get("as_sort","desc"))
        self.var_limit.set(c.get("post_limit","200"))
        self.var_depth.set(int(c.get("comment_depth",2)))
        self.var_max_cmt.set(c.get("max_comments","30"))
        self.var_outdir.set(c.get("output_dir",DEFAULT_CONFIG["output_dir"]))
        self.var_min_comments.set(c.get("min_comments","0"))
        self.var_min_upvotes.set(c.get("min_upvotes","0"))
        self.var_min_ratio.set(c.get("min_ratio","0"))
        self.var_min_resonance.set(c.get("min_resonance","0"))
        self.var_max_resonance.set(c.get("max_resonance","100"))
        self._switch_mode(c.get("mode","subreddit"))
        self.var_preset.set(c.get("preset","Custom"))

    def _collect(self):
        return {
            **self.cfg,
            "mode":          self._mode.get(),
            "subreddit":     clean_sub(self.var_sub.get()),
            "post_type":     self.var_post_type.get(),
            "time_filter":   self.var_time.get(),
            "query":         self.var_query.get().strip(),
            "sort":          self.var_sort.get(),
            "as_query":      self.var_as_query.get().strip(),
            "as_subreddit":  clean_sub(self.var_as_sub.get()).strip(),
            "as_date_from":  self.var_as_from.get().strip(),
            "as_date_to":    self.var_as_to.get().strip(),
            "as_sort":       self.var_as_sort.get(),
            "post_limit":    self.var_limit.get(),
            "comment_depth": str(self.var_depth.get()),
            "max_comments":  self.var_max_cmt.get(),
            "output_dir":    self.var_outdir.get().strip(),
            "preset":        self.var_preset.get(),
            "min_comments":  self.var_min_comments.get(),
            "min_upvotes":   self.var_min_upvotes.get(),
            "min_ratio":     self.var_min_ratio.get(),
            "min_resonance": self.var_min_resonance.get(),
            "max_resonance": self.var_max_resonance.get(),
        }

    def _apply_preset(self,*_):
        p=PRESETS.get(self.var_preset.get(),{})
        if "post_limit"    in p: self.var_limit.set(p["post_limit"])
        if "comment_depth" in p: self.var_depth.set(int(p["comment_depth"]))

    # ── Logging & UI ──────────────────────────────────────────────────────────
    def _check_deps(self):
        missing=(["praw"] if not PRAW_OK else [])+(["vaderSentiment"] if not VADER_OK else [])
        if missing:
            self._log(f"⚠ Missing: {', '.join(missing)}  →  pip install {' '.join(missing)}","warn")
        else:
            self._log("All packages OK — ready.","sub")

    def _log(self,msg,tag=None):
        def _do():
            self.log_box.config(state="normal")
            ts=datetime.now().strftime("%H:%M:%S")
            self.log_box.insert("end",f"{ts}  ","ts")
            self.log_box.insert("end",f"{msg}\n",tag or "")
            self.log_box.see("end")
            self.log_box.config(state="disabled")
        self.after(0,_do)

    def _set_progress(self,cur,total,label=""):
        def _do():
            self.progressbar["value"]=int(100*cur/total) if total else 0
            self.prog_lbl.config(text=label or f"{cur}/{total}")
        self.after(0,_do)

    def _set_stats(self,scraped,skipped,pos,neg,neutral=0,comments=0):
        def _do():
            self.stat_vars["Scraped"].set(str(scraped))
            self.stat_vars["Skipped"].set(str(skipped))
            self.stat_vars["Positive"].set(str(pos))
            self.stat_vars["Negative"].set(str(neg))
            self.stat_vars["Neutral"].set(str(neutral))
            self.stat_vars["Comments"].set(str(comments))
        self.after(0,_do)

    def _set_date_progress(self, current_date_str, date_from, date_to):
        """Update the date progress card. All args are YYYY-MM-DD strings."""
        def _do():
            try:
                from datetime import date as _date
                d_cur =_date.fromisoformat(str(current_date_str)[:10])
                d_from=_date.fromisoformat(str(date_from)[:10])
                d_to  =_date.fromisoformat(str(date_to)[:10])
                total =(d_to-d_from).days
                # desc sort = newest first, so cursor moves backwards
                # pct = how far from d_to back towards d_from
                done  =(d_to-d_cur).days
                pct   =max(0,min(100,int(100*done/total))) if total>0 else 0
                self.var_date_progress.set(str(d_cur))
                self.var_date_pct.set(f"{pct}%  |  {str(d_cur)}")
                self.date_progressbar["value"]=pct
            except Exception as e:
                self.var_date_progress.set(str(current_date_str)[:10])
                self.var_date_pct.set("?")
        self.after(0,_do)

    def _set_status(self,text,clr=None):
        self.after(0,lambda: self.status_pill.config(text=f"● {text}",fg=clr or MUTED))

    # ── Start / Stop ──────────────────────────────────────────────────────────
    def _start(self):
        if self._is_running: return
        cfg=self._collect()
        mode=cfg["mode"]

        if mode in ("subreddit","search") and not PRAW_OK:
            messagebox.showerror("Missing package","Run:  pip install praw vaderSentiment")
            return
        if mode=="subreddit" and not cfg["subreddit"]:
            messagebox.showwarning("Input needed","Enter a subreddit name."); return
        if mode=="search" and not cfg["query"]:
            messagebox.showwarning("Input needed","Enter a search query."); return
        if mode=="arctic":
            if not cfg["as_query"] and not cfg["as_subreddit"]:
                messagebox.showwarning("Input needed",
                    "Arctic Shift needs at least a keyword OR a subreddit.\n"
                    "Both can be empty but the API will reject the request."); return
            # Validate date format
            import re as _re
            for field,val in [("Date From",cfg["as_date_from"]),("Date To",cfg["as_date_to"])]:
                if val and not _re.match(r"\d{4}-\d{2}-\d{2}",val):
                    messagebox.showwarning("Date format",
                        f"{field} must be YYYY-MM-DD  (e.g. 2023-01-01)"); return

        self.cfg=cfg; save_config(cfg)
        self._stop_flag.clear(); self._is_running=True
        self.btn_start.set_enabled(False); self.btn_stop.set_enabled(True)
        self.progressbar["value"]=0; self._set_stats(0,0,0,0)
        self.var_date_progress.set("—")
        self.var_date_pct.set("")
        self.date_progressbar["value"]=0
        self._set_status("Running", ARCTIC if mode=="arctic" else ACCENT)

        tag = "arctic" if mode=="arctic" else "accent"
        if mode=="subreddit":
            self._log(f"Subreddit  r/{cfg['subreddit']}  [{cfg['post_type']} · {cfg['post_limit']} posts]",tag)
        elif mode=="search":
            self._log(f"Search  \"{cfg['query']}\"  [sort={cfg['sort']} · {cfg['post_limit']} posts]",tag)
        else:
            self._log(f"Arctic Shift  \"{cfg['as_query']}\"  [{cfg['as_date_from']} → {cfg['as_date_to']}]",tag)

        threading.Thread(target=self._run,args=(cfg,),daemon=True).start()

    def _stop(self):
        if not self._is_running: return
        self._stop_flag.set()
        self._log("Stop requested — saving collected data…","warn")
        self._set_status("Stopping…",WARNING)
        self.btn_stop.set_enabled(False)

    def _done(self):
        self._is_running=False
        self.btn_start.set_enabled(True)
        self.btn_stop.set_enabled(False)

    # ── Shared post processing ────────────────────────────────────────────────
    def _apply_filters(self,cfg,n_comments,score,upvote_ratio):
        """Pre-fetch filters. Returns True if post should be skipped."""
        try:
            min_c=int(cfg.get("min_comments",0) or 0)
            min_u=int(cfg.get("min_upvotes",0)  or 0)
            min_r=float(cfg.get("min_ratio",0)   or 0)/100.0
        except ValueError:
            min_c=min_u=0; min_r=0.0
        return (n_comments<min_c or score<min_u or upvote_ratio<min_r)

    def _apply_pain_filter(self,cfg,pain_score):
        try:
            mn=int(cfg.get("min_resonance",0)   or 0)
            mx=int(cfg.get("max_resonance",100) or 100)
        except ValueError:
            mn=0; mx=100
        return pain_score<mn or pain_score>mx

    def _process_comments_praw(self,post,depth,max_write):
        fetch_cap=max_write*4
        def load():
            post.comments.replace_more(limit=depth)
            out=[]
            for c in post.comments.list():
                if not isinstance(c,praw.models.Comment): continue
                if len(out)>=fetch_cap: break
                cdepth=getattr(c,'_depth',None)
                if cdepth is None:
                    cdepth=0; anc=c.parent()
                    while isinstance(anc,praw.models.Comment): cdepth+=1; anc=anc.parent()
                out.append({"id":c.id,"parent_id":c.parent_id,"body":c.body,
                            "author":str(c.author) if c.author else "[deleted]",
                            "score":c.score,"depth":cdepth,
                            "date":datetime.utcfromtimestamp(c.created_utc).strftime('%Y-%m-%d %H:%M')})
            return out
        return fetch_with_retry(load,log_fn=self._log)

    def _fetch_arctic_comments(self,post_id,max_write):
        """Fetch comments from Arctic Shift API for a given post id."""
        fetch_cap=max_write*4
        try:
            # link_id needs t3_ prefix; sort must be asc or desc
            link_id = post_id if post_id.startswith("t3_") else f"t3_{post_id}"
            url=("https://arctic-shift.photon-reddit.com/api/comments/tree"
                 f"?link_id={link_id}&limit={fetch_cap}")
            self._log(f"  comments → {url}", "sub")
            req=urllib.request.Request(url,headers={"User-Agent":"UnifiedScraper/1.0"})
            try:
                with urllib.request.urlopen(req,timeout=15) as resp:
                    raw_resp=resp.read()
            except urllib.error.HTTPError as he:
                err_body=""
                try: err_body=he.read().decode()[:300]
                except: pass
                self._log(f"  ✗ {he.code} {he.reason}: {err_body}","error")
                return []
            data=json.loads(raw_resp)
            # Log structure of first response to diagnose parsing
            if not hasattr(self,'_as_comment_logged'):
                self._as_comment_logged=True
                keys=list(data.keys())
                raw_preview=json.dumps(data)[:400]
                self._log(f"  comment API keys: {keys}","sub")
                self._log(f"  comment API preview: {raw_preview}","sub")

            # API returns {"data": [...]} where each item is a comment
            # Items may be nested with "replies" list, or flat list
            raw=data.get("data",[])
            if not raw:
                # try alternate key names
                raw=data.get("comments",data.get("children",[]))

            out=[]
            def walk(items,depth=0):
                for item in items:
                    if not isinstance(item,dict): continue
                    # skip "more" stub items
                    if item.get("kind")=="more": continue
                    # comment body may be in "body" or "data.body"
                    body=item.get("body","")
                    if not body and isinstance(item.get("data"),dict):
                        body=item["data"].get("body","")
                    if not body or body in ("[deleted]","[removed]"): continue
                    pid=item.get("parent_id","")
                    cid=item.get("id","")
                    if not cid and isinstance(item.get("data"),dict):
                        cid=item["data"].get("id","")
                        pid=item["data"].get("parent_id","")
                    ts=item.get("created_utc",0) or (item.get("data") or {}).get("created_utc",0)
                    try: date_str=datetime.utcfromtimestamp(float(ts)).strftime('%Y-%m-%d %H:%M')
                    except: date_str="?"
                    score=item.get("score",0) or (item.get("data") or {}).get("score",0)
                    author=item.get("author","[deleted]") or (item.get("data") or {}).get("author","[deleted]")
                    out.append({"id":cid,"parent_id":pid,"body":sanitize(body),
                                "author":author,
                                "score":int(score) if score else 0,
                                "depth":depth,
                                "date":date_str})
                    if len(out)>=fetch_cap: return
                    # replies may be nested list or {"data":{"children":[...]}}
                    replies=item.get("replies",[])
                    if isinstance(replies,dict):
                        replies=replies.get("data",{}).get("children",[])
                    walk(replies,depth+1)
            walk(raw)
            return out
        except Exception as e:
            self._log(f"  ⚠ Comment fetch failed for {post_id}: {e}","warn")
            return []

    def _build_row(self,post_id,title,body,score,upvote_ratio,awards,
                   crossposts,is_oc,url,author,subreddit,flair,
                   n_comments,created_utc,comment_list):
        full=sanitize(f"{title}\n\n{body}")
        ps=get_sentiment(full)
        cs=get_sentiment(" ".join(c["body"] for c in comment_list))
        res=calc_pain_score(created_utc,comment_list,score,n_comments,ps)
        try:
            dt=datetime.utcfromtimestamp(created_utc)
            date_str=dt.strftime('%Y-%m-%d'); time_str=dt.strftime('%H:%M:%S')
        except:
            date_str="?"; time_str="?"
        return ps,cs,res,{
            "post_id":post_id,"title":sanitize(title),"body":sanitize(body),
            "upvotes":score,"upvote_ratio":round(upvote_ratio,3),
            "awards":awards,"crossposts":crossposts,"is_oc":is_oc,
            "url":url,"author":author,"subreddit":subreddit,
            "flair":flair or "","n_comments":n_comments,
            "post_sentiment":ps,"comments_sentiment":cs,
            "date":date_str,"time":time_str,
            "_comments":comment_list,"_resonance":res,
        }

    # ── Main scrape dispatcher ─────────────────────────────────────────────────
    def _run(self,cfg):
        mode=cfg["mode"]
        try:
            if mode=="subreddit": self._run_subreddit(cfg)
            elif mode=="search":  self._run_search(cfg)
            else:                 self._run_arctic(cfg)
        except Exception as e:
            self._log(f"✗ Fatal: {e}","error"); self._set_status("Error",ERROR)
        self.after(0,self._done)

    def _run_core(self,cfg,posts_iter,fpath,source_label):
        """Shared post loop for subreddit and search modes."""
        limit=max(1,min(1000,int(cfg["post_limit"] or 200)))
        depth=max(0,min(10,  int(cfg["comment_depth"] or 2)))
        max_write=int(cfg["max_comments"] or 30)
        outdir=cfg["output_dir"]; os.makedirs(outdir,exist_ok=True)
        self._log(f"Arctic Shift — limit={limit}  depth={depth}  max_comments={max_write}","sub")
        ids_path=fpath.replace(".txt",".ids")
        existing=set()
        if os.path.exists(ids_path):
            try:
                with open(ids_path,"r",encoding="utf-8") as f: existing=set(f.read().splitlines())
                self._log(f"Existing file — {len(existing)} posts already saved","sub")
            except: pass

        rows=[]; scraped=skipped=pos=neg=neutral=total_comments=0

        for post in posts_iter:
            if self._stop_flag.is_set(): break
            if post.id in existing:
                skipped+=1; self._set_progress(scraped,limit,f"{scraped} scraped · {skipped} skipped"); continue
            if self._apply_filters(cfg,post.num_comments,post.score,post.upvote_ratio):
                skipped+=1; self._set_progress(scraped,limit,f"{scraped} scraped · {skipped} filtered"); continue

            try:
                body=post.selftext if post.is_self else "[Link Post]"
                clist=self._process_comments_praw(post,depth,max_write)
                ps,cs,res,row=self._build_row(
                    post.id,post.title,body,post.score,post.upvote_ratio,
                    post.total_awards_received,post.num_crossposts,
                    post.is_original_content,post.url,str(post.author),
                    post.subreddit.display_name,post.link_flair_text,
                    post.num_comments,post.created_utc,clist)

                if self._apply_pain_filter(cfg,res["score"]):
                    skipped+=1; self._set_progress(scraped,limit,f"{scraped} scraped · {skipped} filtered"); continue

                if ps=="Positive": pos+=1
                elif ps=="Negative": neg+=1
                else: neutral+=1
                total_comments+=len(clist)
                rows.append(row); existing.add(post.id); scraped+=1
                self._set_progress(scraped,limit,f"{scraped} scraped · {skipped} skipped")
                self._set_stats(scraped,skipped,pos,neg,neutral,total_comments)
                if scraped%25==0: self._log(f"{scraped} posts collected…","sub")
            except Exception as e:
                self._log(f"⚠ Skipped {post.id}: {e}","warn")

        self._finish(rows,fpath,cfg,scraped,skipped,pos,neg,neutral,total_comments)

    def _run_subreddit(self,cfg):
        reddit=praw.Reddit(client_id=cfg["client_id"],client_secret=cfg["client_secret"],
                           user_agent=cfg["user_agent"])
        try:
            sub=reddit.subreddit(cfg["subreddit"]); _=sub.title
        except Exception as e:
            self._log(f"✗ Subreddit error: {e}","error"); self._set_status("Error",ERROR); return

        limit=max(1,min(1000,int(cfg["post_limit"] or 200)))
        tf=cfg["time_filter"]; pt=cfg["post_type"]
        gens={"new":lambda: sub.new(limit=limit),"hot":lambda: sub.hot(limit=limit),
              "rising":lambda: sub.rising(limit=limit),
              "top":lambda: sub.top(limit=limit,time_filter=tf),
              "controversial":lambda: sub.controversial(limit=limit,time_filter=tf)}

        fname=f"{cfg['subreddit']}_{pt}_posts.txt".replace(" ","_")
        fpath=os.path.join(cfg["output_dir"],fname)
        self._run_core(cfg,gens[pt](),fpath,f"r/{cfg['subreddit']}")

    def _run_search(self,cfg):
        reddit=praw.Reddit(client_id=cfg["client_id"],client_secret=cfg["client_secret"],
                           user_agent=cfg["user_agent"])
        limit=max(1,min(1000,int(cfg["post_limit"] or 200)))
        posts_gen=reddit.subreddit("all").search(
            cfg["query"],sort=cfg["sort"],time_filter=cfg["time_filter"],limit=limit)
        safe=re.sub(r'[^\w\-]','_',cfg["query"])[:40]
        fname=f"search_{safe}_{cfg['sort']}.txt"
        fpath=os.path.join(cfg["output_dir"],fname)
        self._run_core(cfg,posts_gen,fpath,f"search:{cfg['query']}")

    def _run_arctic(self,cfg):
        limit=max(1,min(1000,int(cfg["post_limit"] or 200)))
        depth=max(0,min(10,  int(cfg["comment_depth"] or 2)))
        max_write=int(cfg["max_comments"] or 30)
        outdir=cfg["output_dir"]; os.makedirs(outdir,exist_ok=True)
        self._log(f"Arctic Shift — limit={limit}  depth={depth}  max_comments={max_write}","sub")

        safe=re.sub(r'[^\w\-]','_',cfg["as_query"] or cfg["as_subreddit"])[:40]
        fname=f"arctic_{safe}.txt"
        fpath=os.path.join(outdir,fname)
        ids_path=fpath.replace(".txt",".ids")
        existing=set()
        if os.path.exists(ids_path):
            try:
                with open(ids_path,"r",encoding="utf-8") as f: existing=set(f.read().splitlines())
                self._log(f"Existing file — {len(existing)} posts already saved","sub")
            except: pass

        # Paginate Arctic Shift API
        # Arctic Shift: sort=asc|desc only (always sorts by created_utc)
        params={"limit":100,"sort":cfg["as_sort"]}
        if cfg["as_query"]:     params["q"]         = cfg["as_query"]
        if cfg["as_subreddit"]: params["subreddit"] = cfg["as_subreddit"]
        if cfg["as_date_from"]: params["after"]     = cfg["as_date_from"]
        if cfg["as_date_to"]:   params["before"]    = cfg["as_date_to"]

        # Log what we're querying so user can verify before waiting
        q_val    = params.get("q", "")
        sub_val  = params.get("subreddit", "")
        q_info   = ('q="' + q_val + '"') if q_val else "(no keyword)"
        sub_info = ("  r/" + sub_val) if sub_val else "  (all subreddits)"
        date_rng = params.get("after","?") + " to " + params.get("before","?")
        self._log("Querying Arctic Shift: " + q_info + sub_info + "  " + date_rng, "arctic")

        rows=[]; scraped=skipped=pos=neg=neutral=total_comments=0
        page=0; last_cursor=None
        sort_dir=params.get("sort","desc")

        while scraped<limit and not self._stop_flag.is_set():
            page_params=dict(params)
            if last_cursor is not None:
                # desc = newest first → move 'before' backwards each page
                # asc  = oldest first → move 'after'  forwards  each page
                if sort_dir=="desc":
                    page_params["before"]=last_cursor
                else:
                    page_params["after"]=last_cursor
            qs=urllib.parse.urlencode(page_params)
            url=f"https://arctic-shift.photon-reddit.com/api/posts/search?{qs}"
            if page==0: self._log(f"→ {url[:120]}","sub")
            try:
                req=urllib.request.Request(url,headers={"User-Agent":"UnifiedScraper/1.0"})
                with urllib.request.urlopen(req,timeout=20) as resp:
                    raw=resp.read()
                data=json.loads(raw)
                if "error" in data:
                    self._log(f"✗ Arctic Shift: {data['error']}","error"); break
                batch=data.get("data",[])
                if not batch:
                    self._log("Arctic Shift: no more posts — done.","sub"); break
            except urllib.error.HTTPError as e:
                body_msg=""
                try: body_msg=e.read().decode()[:300]
                except: pass
                self._log(f"✗ HTTP {e.code} {e.reason}  {body_msg}","error"); break
            except Exception as e:
                self._log(f"✗ Arctic Shift error: {e}","error"); break

            for p in batch:
                if self._stop_flag.is_set(): break
                if scraped>=limit: break
                post_id=p.get("id","")
                if not post_id or post_id in existing:
                    skipped+=1; continue

                score      =int(p.get("score",0))
                n_comments =int(p.get("num_comments",0))
                upv_ratio  =float(p.get("upvote_ratio",0.5))

                if self._apply_filters(cfg,n_comments,score,upv_ratio):
                    skipped+=1; self._set_progress(scraped,limit,f"{scraped} scraped · {skipped} filtered"); continue

                title  =p.get("title","")
                body   =p.get("selftext","") or "[Link Post]"
                purl   =p.get("url","")
                author =p.get("author","[deleted]")
                sub    =p.get("subreddit","")
                flair  =p.get("link_flair_text","")
                awards =int(p.get("total_awards_received",0))
                crosp  =int(p.get("num_crossposts",0))
                is_oc  =bool(p.get("is_original_content",False))
                try:    created=float(p.get("created_utc",0))
                except: created=0.0

                # Update date card + progress label on every post
                try:
                    cur_date=datetime.utcfromtimestamp(created).strftime("%b %Y")
                    cur_date_iso=datetime.utcfromtimestamp(created).strftime("%Y-%m-%d")
                    df=cfg.get("as_date_from","") or "2020-01-01"
                    dt=cfg.get("as_date_to","")   or "2025-01-01"
                    self._set_date_progress(cur_date_iso, df, dt)
                except: cur_date="?"
                self._set_progress(scraped,limit,
                    f"{scraped}/{limit} scraped · {skipped} skipped · {cur_date}")

                clist=self._fetch_arctic_comments(post_id,max_write)
                total_comments+=len(clist)
                time.sleep(0.3)  # polite delay

                ps,cs,res,row=self._build_row(
                    post_id,title,body,score,upv_ratio,awards,crosp,is_oc,
                    purl,author,sub,flair,n_comments,created,clist)

                if self._apply_pain_filter(cfg,res["score"]):
                    skipped+=1
                    continue

                if ps=="Positive": pos+=1
                elif ps=="Negative": neg+=1
                else: neutral+=1
                rows.append(row); existing.add(post_id); scraped+=1
                self._set_stats(scraped,skipped,pos,neg,neutral,total_comments)
                if scraped%25==0:
                    self._log(f"{scraped} posts · {cur_date}","arctic")

            if batch:
                try:
                    ts=int(float(batch[-1].get("created_utc",0)))
                    # desc: slide 'before' window back (subtract 1 second)
                    # asc:  slide 'after'  window forward (add 1 second)
                    last_cursor=str(ts-1) if sort_dir=="desc" else str(ts+1)
                except: pass
                # Pre-register all batch IDs to avoid re-processing posts
                # whose timestamp lands inside the next cursor window
                for _p in batch:
                    _pid = _p.get("id","")
                    if _pid: existing.add(_pid)
            page+=1
            # Show current date position and filter summary
            if batch:
                try:
                    oldest_in_batch = datetime.utcfromtimestamp(
                        float(batch[-1].get("created_utc",0))).strftime("%Y-%m-%d")
                    newest_in_batch = datetime.utcfromtimestamp(
                        float(batch[0].get("created_utc",0))).strftime("%Y-%m-%d")
                except:
                    oldest_in_batch = newest_in_batch = "?"
                total_seen = scraped + skipped
                filter_rate = f"{skipped}/{total_seen} filtered" if total_seen else ""
                self._log(
                    f"  page {page} — scraped={scraped}  {filter_rate}"
                    f"  ·  date range this page: {oldest_in_batch} → {newest_in_batch}",
                    "sub")
            # NOTE: do NOT break on partial batch — Arctic Shift can return <100
            # even mid-range when filters reduce the page. Only break on empty batch
            # (already handled above at "no more posts" check).
            time.sleep(0.5)

        self._finish(rows,fpath,cfg,scraped,skipped,pos,neg,neutral,total_comments)

    def _finish(self,rows,fpath,cfg,scraped,skipped,pos,neg,neutral,total_comments):
        if not rows:
            self._log("No posts collected.","warn")
        else:
            self._save_txt(rows,fpath,cfg)
            self._log(f"Saved → {fpath}","sub")

        self._log(f"Done · {scraped} scraped · {skipped} skipped · {total_comments} comments · "
                  f"+{pos} pos · ~{neutral} neutral · -{neg} neg","success")
        self._set_stats(scraped,skipped,pos,neg,neutral,total_comments)
        self._set_status("Done",SUCCESS)
        self.after(0,lambda: self.prog_lbl.config(text="Finished"))
        if rows:
            self.after(0,lambda: self._summary(scraped,skipped,pos,neg,neutral,total_comments,fpath))
        if TOAST_OK and scraped:
            try: ToastNotifier().show_toast("Reddit Scraper",f"Done! {scraped} posts saved.",duration=5,threaded=True)
            except: pass

    # ── TXT digest writer (shared) ────────────────────────────────────────────
    def _save_txt(self,rows,path,cfg):
        ids_path=path.replace(".txt",".ids"); max_cmts=int(cfg.get("max_comments",30))
        existing_ids=set()
        if os.path.exists(ids_path):
            try:
                with open(ids_path,"r",encoding="utf-8") as f: existing_ids=set(f.read().splitlines())
            except: pass

        new_rows=[r for r in rows if r["post_id"] not in existing_ids]
        if not new_rows: self._log("No new posts to append.","sub"); return

        INDENT="    "; SEP="─"*72; THICK="═"*72; lines=[]
        mode=cfg.get("mode","subreddit")

        if not os.path.exists(path):
            if mode=="subreddit":
                header=f"r/{cfg['subreddit']}  ·  {cfg['post_type'].upper()}  ·  time={cfg['time_filter']}"
            elif mode=="search":
                header=f"Search: \"{cfg['query']}\"  ·  sort={cfg['sort']}"
            else:
                header=f"Arctic Shift: \"{cfg['as_query']}\"  ·  {cfg['as_date_from']} → {cfg['as_date_to']}"
            lines+=[THICK,f"  {header}  ·  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                    f"  Max comments per post: {max_cmts}",THICK,""]

        for i,row in enumerate(new_rows,1):
            all_comments=row.get("_comments",[])
            top_level_all=sorted([c for c in all_comments if c["depth"]==0],key=lambda c:-c["score"])
            reply_map={}
            for c in all_comments:
                if c["depth"]==1: reply_map.setdefault(c["parent_id"],[]).append(c)
            for pid in reply_map: reply_map[pid].sort(key=lambda c:-c["score"])
            selected=[]; budget=max_cmts
            for tc in top_level_all:
                if budget<=0: break
                selected.append((tc,"")); budget-=1
                for rep in reply_map.get(f"t1_{tc['id']}",[])[:3]:
                    if budget<=0: break
                    selected.append((rep,INDENT)); budget-=1

            shown=len(selected)
            omitted=len(top_level_all)-sum(1 for c,_ in selected if c["depth"]==0)
            omit_note=f"  ·  +{omitted} omitted" if omitted>0 else ""

            res=row.get("_resonance",{}); rpat=res.get("pattern","?"); rsco=res.get("score",0)
            ricon=("🔥" if rpat=="STRONG PAIN" else "⚡" if rpat=="CLEAR PAIN"
                   else "〰" if rpat=="MILD PAIN" else "·")
            res_line=(f"{ricon} {rpat}  ·  pain {rsco}/100"
                      f"  ·  {res.get('cph_1h',0)} c/h"
                      f"  ·  eng {res.get('eng_ratio',0)}"
                      f"  ·  vpen {res.get('viral_pen',1)}")

            si={"Positive":"▲","Negative":"▼","Neutral":"●"}.get(row["post_sentiment"],"●")
            lines+=["",SEP,
                    f"POST #{i}  ·  {si} {row['post_sentiment'].upper()}"
                    f"  ·  ▲{row['upvotes']} upvotes  ·  {int(row['upvote_ratio']*100)}% ratio",
                    f"PAIN      : {res_line}",SEP,
                    f"Title     : {row['title']}",
                    f"Subreddit : r/{row['subreddit']}",
                    f"Author    : u/{row['author']}  ·  {row['date']} {row['time']}",
                    f"URL       : {row['url']}"]
            if row["flair"]:   lines.append(f"Flair     : {row['flair']}")
            if row["awards"]>0: lines.append(f"Awards    : {row['awards']}")
            lines.append(f"Comments  : {row['n_comments']} total  ·  showing {shown}{omit_note}"
                         f"  ·  sentiment: {row['comments_sentiment']}")

            if row["body"] and row["body"] not in ("[Link Post]",""):
                lines+=["","── POST BODY "+"─"*58,row["body"],""]
            else:
                lines+=[f"[{row['body']}]",""]

            if not all_comments:
                lines.append("[No comments scraped]")
            else:
                lines.append("── TOP COMMENTS "+"─"*55)
                def emit(c,indent=""):
                    sb=f"▲{c['score']}" if c["score"]>=0 else f"▼{abs(c['score'])}"
                    lines.append(f"{indent}[{sb}  u/{c['author']}  ·  {c['date']}]")
                    for para in c["body"].split("\n"):
                        lines.append(f"{indent}  {para}" if para.strip() else "")
                    lines.append("")
                for c,indent in selected: emit(c,indent=indent)

        lines.append("")
        mode2="a" if os.path.exists(path) else "w"
        with open(path,mode2,encoding="utf-8") as f: f.write("\n".join(lines))
        all_ids=existing_ids|{r["post_id"] for r in new_rows}
        with open(ids_path,"w",encoding="utf-8") as f: f.write("\n".join(sorted(all_ids)))
        self._log(f"{len(new_rows)} posts written  ·  max {max_cmts} comments each","sub")

    # ── Summary dialog ────────────────────────────────────────────────────────
    def _summary(self,scraped,skipped,pos,neg,neutral,total_comments,path):
        win=tk.Toplevel(self); win.title("Complete"); win.configure(bg=PANEL)
        win.resizable(False,False); win.grab_set()
        tk.Label(win,text="Scrape complete",font=("Segoe UI Semibold",13),
                 bg=PANEL,fg=TEXT).pack(pady=(24,4),padx=32)
        mode=self.cfg.get("mode","subreddit")
        subtitle=(f"r/{self.cfg['subreddit']}" if mode=="subreddit"
                  else f"\"{self.cfg['query']}\"" if mode=="search"
                  else f"Arctic: \"{self.cfg['as_query']}\"")
        tk.Label(win,text=subtitle,font=FN_SUB,bg=PANEL,fg=MUTED).pack(pady=(0,14))
        for label,val,clr in [
            ("Posts scraped",scraped,SUCCESS),("Duplicates skipped",skipped,SUBTEXT),
            ("Comments scraped",total_comments,"#7c8cf8"),
            ("Positive",pos,SUCCESS),("Neutral",neutral,SUBTEXT),("Negative",neg,ERROR)]:
            r=tk.Frame(win,bg=PANEL); r.pack(fill="x",padx=32,pady=2)
            tk.Label(r,text=label,font=FN_SUB,bg=PANEL,fg=SUBTEXT,width=20,anchor="w").pack(side="left")
            tk.Label(r,text=str(val),font=("Segoe UI Semibold",10),bg=PANEL,fg=clr).pack(side="left")
        _hsep(win,BORDER).pack(fill="x",padx=32,pady=12)
        tk.Label(win,text=path,font=FN_MONO,bg=PANEL,fg=MUTED,
                 wraplength=380,justify="left").pack(padx=32,pady=(0,14))
        br=tk.Frame(win,bg=PANEL); br.pack(pady=(0,20),padx=32)
        tk.Button(br,text="Open file",font=FN_BTN,bg=ACCENT,fg=TEXT,relief="flat",
                  padx=14,pady=6,cursor="hand2",bd=0,activebackground=ACCENT2,
                  command=lambda: _open_file(path)
                  ).pack(side="left",padx=(0,8))
        tk.Button(br,text="Close",font=FN_BTN,bg=INPUT_BG,fg=TEXT,relief="flat",
                  padx=14,pady=6,cursor="hand2",bd=0,activebackground=HOVER,
                  command=win.destroy).pack(side="left")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = App()
    app.mainloop()
